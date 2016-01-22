#include "base.h"

/* Variable Definitions */
struct rte_mempool *request_mempool = NULL;
struct ctrlr_entry *g_controllers = NULL;

struct ns_entry *g_namespaces = NULL;
int g_num_namespaces = 0;
struct worker_thread *g_workers = NULL;
int g_num_workers = 0;
uint64_t g_tsc_rate = 0;
int g_io_size_bytes = 0;
int g_time_in_sec = 0;
uint32_t g_max_completions = 0;
const char *g_core_mask = NULL;

uint64_t f_len = 0;
uint64_t f_maxasu = 0;
uint64_t f_maxlba = 0;
uint64_t f_maxsize = 0;
uint64_t f_totalblocks = 0;
uint64_t iotask_read_count = 0;
uint64_t iotask_write_count = 0;


struct perf_task *task;
struct iotask *iotasks;

/* Function Definitions */


static void
task_complete(struct perf_task *task){
	struct ns_worker_ctx	*ns_ctx;

	ns_ctx = task->ns_ctx;
	ns_ctx->current_queue_depth--;
	ns_ctx->io_completed++;

	//rte_mempool_put(task_pool, task);
}

static void
io_complete(void *ctx, const struct nvme_completion *completion){
	task_complete((struct perf_task *)ctx);
}

static int
submit_read(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_read(entry->u.nvme.ns, task->buf, lba,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}

static int
submit_write(struct ns_worker_ctx *ns_ctx, struct perf_task *task, uint64_t lba, uint64_t num_blocks){
	struct ns_entry	*entry = ns_ctx->entry;
	int rc;

	rc = nvme_ns_cmd_write(entry->u.nvme.ns, task->buf, lba,
		num_blocks, io_complete, task);
	ns_ctx->current_queue_depth++;

	return rc;
}


static int
work_fn(void *arg){
	struct worker_thread *worker = (struct worker_thread *)arg;
	struct ns_worker_ctx *ns_ctx = worker->ns_ctx;
	int rc;
	ns_ctx->current_queue_depth = 0;

	printf("Starting thread on core %u\n", worker->lcore);

	if (nvme_register_io_thread() != 0) {
		fprintf(stderr, "nvme_register_io_thread() failed on core %u\n", worker->lcore);
		return -1;
	}
	
	task = rte_malloc(NULL, sizeof(struct perf_task), 0x200);
	task->buf = rte_malloc(NULL, MAX_IO_BLOCKS*512*2, 0x200);	
	task->ns_ctx = ns_ctx;

	printf("We should see this message\n");
	uint64_t tsc_start = rte_get_timer_cycles();

	for (uint64_t i = 0; i < f_len; i++){

		if (ns_ctx->current_queue_depth > 0){
			nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);			
			while (ns_ctx->current_queue_depth > 0){
				nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);		
			}
		}
		uint64_t addr = iotasks[i].asu * f_maxlba + iotasks[i].lba;
		if (iotasks[i].type == 0){
			rc = submit_read(ns_ctx, task, addr, iotasks[i].size);		
			if (rc != 0){
				printf("Error at submiting command %lu\n", i);
				exit(1);
			}
		}	else{
			rc = submit_write(ns_ctx, task, addr, iotasks[i].size);		
			if (rc != 0){
				printf("Error at submiting command %lu\n", i);
				exit(1);
			}
		}	
		if ((i % 100000) == 0) printf("Reach %lu %lu\n", i, ns_ctx->current_queue_depth);
	}

	while (ns_ctx->current_queue_depth > 0){
		nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
	}

	uint64_t tsc_end = rte_get_timer_cycles();
	double sec = (tsc_end - tsc_start)/ (double)g_tsc_rate;
	printf("Time %lf second\n", sec);
	printf("Throughput %lf MB/s\n", 0.5*(double)f_totalblocks/1024/sec);
	rte_free(task);
	nvme_unregister_io_thread();
	return 0;
}

int main(void){
	/* Launch all of the slave workers */

	int rc;
	rc = initSPDK();
	if (rc != 0){
		printf("Init SPDK&DPDK error \n");
		exit(1);
	}
	
	rc = initTrace();
	if (rc != 0){
		printf("Init trace file error \n");
		exit(1);
	}
	
	rc = work_fn(g_workers);
	free(iotasks);
	unregister_controllers();


}
