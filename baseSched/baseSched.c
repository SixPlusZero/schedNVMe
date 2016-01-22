#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>

#include <pciaccess.h>

#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
#include <rte_lcore.h>

#include "spdk/file.h"
#include "spdk/nvme.h"
#include "spdk/pci.h"
#include "spdk/string.h"
#include "../lib/nvme/nvme_impl.h"
#include "../lib/nvme/nvme_internal.h"
#define MAX_IO_BLOCKS 100
#define IO_TIME 100000
typedef unsigned long int   uint64_t;

struct ctrlr_entry {
	struct nvme_controller	*ctrlr;
	struct ctrlr_entry	*next;
	char			name[1024];
};

enum entry_type {
	ENTRY_TYPE_NVME_NS,
	ENTRY_TYPE_AIO_FILE,
};

struct ns_entry {
	enum entry_type		type;

	union {
		struct {
			struct nvme_controller	*ctrlr;
			struct nvme_namespace	*ns;
		} nvme;
	} u;

	struct ns_entry		*next;
	uint32_t		io_size_blocks;
	uint64_t		size_in_ios;
	char			name[1024];
};

struct ns_worker_ctx {
	struct ns_entry		*entry;
	uint64_t		io_completed;
	uint64_t		current_queue_depth;
	uint64_t		offset_in_ios;
	bool			is_draining;

	struct ns_worker_ctx	*next;
};

struct perf_task {
	struct ns_worker_ctx	*ns_ctx;
	void			*buf;
};

struct worker_thread {
	struct ns_worker_ctx 	*ns_ctx;
	struct worker_thread	*next;
	unsigned		lcore;
};

struct rte_mempool *request_mempool;
static struct rte_mempool *task_pool;

static struct ctrlr_entry *g_controllers = NULL;
static struct ns_entry *g_namespaces = NULL;
static int g_num_namespaces = 0;
static struct worker_thread *g_workers = NULL;
static int g_num_workers = 0;

static uint64_t g_tsc_rate;

static int g_io_size_bytes;
static int g_time_in_sec;
static uint32_t g_max_completions;

static const char *g_core_mask;

static void
task_complete(struct perf_task *task);

static void
register_ns(struct nvme_controller *ctrlr, struct pci_device *pci_dev, struct nvme_namespace *ns)
{
	struct ns_entry *entry;
	const struct nvme_controller_data *cdata;

	entry = malloc(sizeof(struct ns_entry));
	if (entry == NULL) {
		perror("ns_entry malloc");
		exit(1);
	}

	cdata = nvme_ctrlr_get_data(ctrlr);

	entry->type = ENTRY_TYPE_NVME_NS;
	entry->u.nvme.ctrlr = ctrlr;
	entry->u.nvme.ns = ns;
	entry->size_in_ios = nvme_ns_get_size(ns) /
			     g_io_size_bytes;
	entry->io_size_blocks = g_io_size_bytes / nvme_ns_get_sector_size(ns);
	
	printf("Namespace %llu %llu\n", entry->size_in_ios, entry->io_size_blocks);
	printf("IO_QUEUE_NUMBER %lu\n", ctrlr->num_io_queues);
	snprintf(entry->name, 44, "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	g_num_namespaces++;
	entry->next = g_namespaces;
	g_namespaces = entry;
}

static void
register_ctrlr(struct nvme_controller *ctrlr, struct pci_device *pci_dev)
{
	int nsid, num_ns;
	struct ctrlr_entry *entry = malloc(sizeof(struct ctrlr_entry));

	if (entry == NULL) {
		perror("ctrlr_entry malloc");
		exit(1);
	}

	entry->ctrlr = ctrlr;
	entry->next = g_controllers;
	g_controllers = entry;

	num_ns = nvme_ctrlr_get_num_ns(ctrlr);
	for (nsid = 1; nsid <= num_ns; nsid++) {
		register_ns(ctrlr, pci_dev, nvme_ctrlr_get_ns(ctrlr, nsid));
	}

}

static void task_ctor(struct rte_mempool *mp, void *arg, void *__task, unsigned id)
{
	struct perf_task *task = __task;
	task->buf = rte_malloc(NULL, g_io_size_bytes, 0x200);
	if (task->buf == NULL) {
		fprintf(stderr, "task->buf rte_malloc failed\n");
		exit(1);
	}
}

static int
parse_args(int argc, char **argv)
{
	/* default value*/
	g_io_size_bytes = 512;
	g_time_in_sec = 0;
	g_core_mask = NULL;
	g_max_completions = 0;

	optind = 1;
	return 0;
}

static int
register_workers(void)
{
	unsigned lcore;
	struct worker_thread *worker;
	struct worker_thread *prev_worker;

	worker = malloc(sizeof(struct worker_thread));
	if (worker == NULL) {
		perror("worker_thread malloc");
		return -1;
	}

	memset(worker, 0, sizeof(struct worker_thread));
	worker->lcore = rte_get_master_lcore();

	g_workers = worker;
	g_num_workers = 1;

	RTE_LCORE_FOREACH_SLAVE(lcore) {
		prev_worker = worker;
		worker = malloc(sizeof(struct worker_thread));
		if (worker == NULL) {
			perror("worker_thread malloc");
			return -1;
		}

		memset(worker, 0, sizeof(struct worker_thread));
		worker->lcore = lcore;
		prev_worker->next = worker;
		g_num_workers++;
	}

	return 0;
}

static int
register_controllers(void)
{
	struct pci_device_iterator	*pci_dev_iter;
	struct pci_device		*pci_dev;
	struct pci_id_match		match;
	int				rc;

	printf("Initializing NVMe Controllers\n");

	pci_system_init();

	match.vendor_id =	PCI_MATCH_ANY;
	match.subvendor_id =	PCI_MATCH_ANY;
	match.subdevice_id =	PCI_MATCH_ANY;
	match.device_id =	PCI_MATCH_ANY;
	match.device_class =	NVME_CLASS_CODE;
	match.device_class_mask = 0xFFFFFF;

	pci_dev_iter = pci_id_match_iterator_create(&match);

	rc = 0;
	while ((pci_dev = pci_device_next(pci_dev_iter))) {
		struct nvme_controller *ctrlr;

		if (pci_device_has_non_null_driver(pci_dev)) {
			fprintf(stderr, "non-null kernel driver attached to nvme\n");
			fprintf(stderr, " controller at pci bdf %d:%d:%d\n",
				pci_dev->bus, pci_dev->dev, pci_dev->func);
			fprintf(stderr, " skipping...\n");
			continue;
		}

		pci_device_probe(pci_dev);
		
		// Here we filter the newly installed NVMe SSD
		printf("[Debug] %d:%d:%d\n", pci_dev->bus, pci_dev->dev, pci_dev->func);
		if (pci_dev->bus != 1) continue;


		ctrlr = nvme_attach(pci_dev);
		
		if (ctrlr == NULL) {
			fprintf(stderr, "nvme_attach failed for controller at pci bdf %d:%d:%d\n",
				pci_dev->bus, pci_dev->dev, pci_dev->func);
			rc = 1;
			continue;
		}

		register_ctrlr(ctrlr, pci_dev);
	}

	pci_iterator_destroy(pci_dev_iter);

	return rc;
}

static void
unregister_controllers(void)
{
	struct ctrlr_entry *entry = g_controllers;

	while (entry) {
		struct ctrlr_entry *next = entry->next;
		nvme_detach(entry->ctrlr);
		free(entry);
		entry = next;
	}
}

static int
associate_workers_with_ns(void)
{
	struct ns_entry		*entry = g_namespaces;
	struct worker_thread	*worker = g_workers;
	struct ns_worker_ctx	*ns_ctx;
	int			i, count;

	count = g_num_namespaces > g_num_workers ? g_num_namespaces : g_num_workers;

	for (i = 0; i < count; i++) {
		ns_ctx = malloc(sizeof(struct ns_worker_ctx));
		if (!ns_ctx) {
			return -1;
		}
		memset(ns_ctx, 0, sizeof(*ns_ctx));

		printf("Associating %s with lcore %d\n", entry->name, worker->lcore);
		ns_ctx->entry = entry;
		ns_ctx->next = worker->ns_ctx;
		worker->ns_ctx = ns_ctx;

		worker = worker->next;
		if (worker == NULL) {
			worker = g_workers;
		}

		entry = entry->next;
		if (entry == NULL) {
			entry = g_namespaces;
		}

	}

	return 0;
}

static char *ealargs[] = {
	"perf",
	"-c 0x1", /* This must be the second parameter. It is overwritten by index in main(). */
	"-n 4",
};

static void
task_complete(struct perf_task *task)
{
	struct ns_worker_ctx	*ns_ctx;

	ns_ctx = task->ns_ctx;
	ns_ctx->current_queue_depth--;
	ns_ctx->io_completed++;

	//rte_mempool_put(task_pool, task);
}

static void
io_complete(void *ctx, const struct nvme_completion *completion)
{
	task_complete((struct perf_task *)ctx);
}

static int
submit_read(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_read(entry->u.nvme.ns, task->buf, lba * entry->io_size_blocks,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}

static int
submit_write(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_write(entry->u.nvme.ns, task->buf, lba * entry->io_size_blocks,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}


static int
work_fn(void *arg){
	struct worker_thread *worker = (struct worker_thread *)arg;
	struct perf_task	*task = NULL;
	struct ns_worker_ctx *ns_ctx = worker->ns_ctx;
	int rc;
	
	ns_ctx->current_queue_depth = 0;

	printf("Starting thread on core %u\n", worker->lcore);

	if (nvme_register_io_thread() != 0) {
		fprintf(stderr, "nvme_register_io_thread() failed on core %u\n", worker->lcore);
		return -1;
	}
	printf("We should see this message\n");

	uint64_t tsc_start = rte_get_timer_cycles();

	if (rte_mempool_get(task_pool, (void **)&task) != 0) {
		fprintf(stderr, "task_pool rte_mempool_get failed\n");
		exit(1);
	}
	task->ns_ctx = ns_ctx;

	for (int i = 1; i <= 100000; i++){

		if (ns_ctx->current_queue_depth > 0){
			nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);			
			while (ns_ctx->current_queue_depth > 0){
				nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);		
			}
		}
		
		
		if ((i & 2) == 0){
			rc = submit_write(ns_ctx, task, i, MAX_IO_BLOCKS);
		} else{
			rc = submit_write(ns_ctx, task, i, MAX_IO_BLOCKS);
		}
		if (rc != 0){
			printf("Error at submit command %d\n", i);
			exit(1);
		}
		if ((i % 50000) == 0) printf("Reach %d %d\n", i, ns_ctx->current_queue_depth);
	}

	while (ns_ctx->current_queue_depth > 0){
		nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
	}
	uint64_t tsc_end = rte_get_timer_cycles();
	double cycles = (tsc_end - tsc_start) * 1000 / (double)g_tsc_rate;
	printf("Time %lf msecond\n", cycles);
	nvme_unregister_io_thread();
	return 0;
}




int main(int argc, char **argv)
{
	int rc;
	struct worker_thread *worker;

	rc = parse_args(argc, argv);
	if (rc != 0) {
		return rc;
	}
	//Here we only use one core (aka. one I/O queue) for our baseline
	g_core_mask = "0x1";
	ealargs[1] = sprintf_alloc("-c %s", g_core_mask ? g_core_mask : "0x1");

	if (ealargs[1] == NULL) {
		perror("ealargs sprintf_alloc");
		return 1;
	}
	rc = rte_eal_init(sizeof(ealargs) / sizeof(ealargs[0]), ealargs);

	free(ealargs[1]);

	if (rc < 0) {
		fprintf(stderr, "could not initialize dpdk\n");
		return 1;
	}
	request_mempool = rte_mempool_create("nvme_request", 8192,
					     nvme_request_size(), 128, 0,
					     NULL, NULL, NULL, NULL,
					     SOCKET_ID_ANY, 0);

	if (request_mempool == NULL) {
		fprintf(stderr, "could not initialize request mempool\n");
		return 1;
	}
	g_io_size_bytes = 512*MAX_IO_BLOCKS;
	task_pool = rte_mempool_create("task_pool", 8192,
				       sizeof(struct perf_task),
				       64, 0, NULL, NULL, task_ctor, NULL,
				       SOCKET_ID_ANY, 0);

	g_tsc_rate = rte_get_timer_hz();

	if (register_workers() != 0) {
		return 1;
	}

	if (register_controllers() != 0) {
		return 1;
	}

	if (associate_workers_with_ns() != 0) {
		return 1;
	}
	
	printf("Initialization complete. Launching workers.\n");

	/* Launch all of the slave workers */


	rc = work_fn(g_workers);

	/*
	worker = g_workers->next;
	while (worker != NULL) {
		rte_eal_remote_launch(work_fn, worker, worker->lcore);
		worker = worker->next;
	}

	rc = work_fn(g_workers);

	worker = g_workers->next;
	while (worker != NULL) {
		if (rte_eal_wait_lcore(worker->lcore) < 0) {
			rc = -1;
		}
		worker = worker->next;
	}

	print_stats();
	*/
	

	unregister_controllers();


}
