#include "base.h"

char *ealargs[] = {
	"perf",
	"-c 0x1", /* This must be the second parameter. It is overwritten by index in main(). */
	"-n 4",
};


void register_ns(struct nvme_controller *ctrlr, struct pci_device *pci_dev, struct nvme_namespace *ns)
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
	
	printf("Namespace %lu %4u\n", entry->size_in_ios, entry->io_size_blocks);
	printf("IO_QUEUE_NUMBER %4u\n", ctrlr->num_io_queues);
	snprintf(entry->name, 44, "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	g_num_namespaces++;
	entry->next = g_namespaces;
	g_namespaces = entry;
}

void register_ctrlr(struct nvme_controller *ctrlr, struct pci_device *pci_dev)
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

void task_ctor(struct rte_mempool *mp, void *arg, void *__task, unsigned id)
{
	struct perf_task *task = __task;
	task->buf = rte_malloc(NULL, g_io_size_bytes, 0x200);
	if (task->buf == NULL) {
		fprintf(stderr, "task->buf rte_malloc failed\n");
		exit(1);
	}
}


int register_workers(void)
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

int register_controllers(void)
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

void unregister_controllers(void)
{
	struct ctrlr_entry *entry = g_controllers;

	while (entry) {
		struct ctrlr_entry *next = entry->next;
		nvme_detach(entry->ctrlr);
		free(entry);
		entry = next;
	}
}

int associate_workers_with_ns(void)
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


int initSPDK(void){
	int rc;
	//struct worker_thread *worker;
	//Here we only use one core (aka. one I/O queue) for our baseline

	g_io_size_bytes = 512;
	g_time_in_sec = 0;
	g_core_mask = NULL;
	g_max_completions = 0;
	optind = 1;
	g_core_mask = "0x1";
	ealargs[1] = sprintf_alloc("-c %s", g_core_mask ? g_core_mask : "0x1");

	if (ealargs[1] == NULL) {
		perror("ealargs sprintf_alloc");
		return 1;
	}
	rc = rte_eal_init(sizeof(ealargs) / sizeof(ealargs[0]), ealargs);

	free(ealargs[1]);
	g_tsc_rate = rte_get_timer_hz();

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
	return 0;
}

int replay_split(struct iotask *dst, char* str){
	int n = 0;
	char *result = NULL;

	char opcode;
	double timestamp;

	result = strtok(str, "\n");
	while(result != NULL){
		sscanf(result, "%u,%u,%u,%c,%lf", 
				&(dst[n].asu), &(dst[n].lba), &(dst[n].size), 
				&opcode, &timestamp);
		dst[n].size /= 512;
		
		// get max asu, lba and size
		f_maxasu = dst[n].asu > f_maxasu ? dst[n].asu : f_maxasu;
		f_maxlba = dst[n].lba + dst[n].size > f_maxlba ? dst[n].lba + dst[n].size : f_maxlba;
		f_maxsize = dst[n].size > f_maxsize ? dst[n].size : f_maxsize;
		// update total size
		f_totalblocks += dst[n].size;
		
		if (opcode == 'r' || opcode == 'R'){
			iotask_read_count++;
			dst[n].type = 0;
		}
		else{
			iotask_write_count++;
			dst[n].type = 1;
		}
	n++;
	result = strtok(NULL, "\n");
	}
	return n;
}


int initTrace(void){
	char *replay_fptr;
	struct stat replay_stat;
	int replay_fd;

	replay_fd = open("WebSearch1.spc", O_RDWR);
	if (replay_fd < 0){
		printf("trace file open failed\n");
		exit(1);
	}

	fstat(replay_fd, &replay_stat);
	replay_fptr = (char *) mmap(NULL, replay_stat.st_size, 
								PROT_READ | PROT_WRITE, MAP_PRIVATE, replay_fd, 0);
	if (replay_fptr == MAP_FAILED){
		printf("trace file mmap failed\n");
		exit(1);
	}
	
	iotasks = malloc(sizeof(struct iotask) * 5000000);
	
	f_len = replay_split(iotasks, replay_fptr);

	munmap(replay_fptr, replay_stat.st_size);
	close(replay_fd);
	printf("%lu %lu %lu %lu\n", f_totalblocks, f_maxasu, f_maxlba, f_maxsize);
	printf("%lu %lu %lu\n", f_len, iotask_read_count, iotask_write_count);

	return 0;
}

void task_complete(struct perf_task *task){
	struct ns_worker_ctx	*ns_ctx;

	ns_ctx = task->ns_ctx;
	ns_ctx->current_queue_depth--;
	ns_ctx->io_completed++;
}

void io_complete(void *ctx, const struct nvme_completion *completion){
	task_complete((struct perf_task *)ctx);
}

int submit_read(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_read(entry->u.nvme.ns, task->buf, lba,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}

int submit_write(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_write(entry->u.nvme.ns, task->buf, lba,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}

