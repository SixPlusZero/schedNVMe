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

struct perf_task *tasks[MAX_NUM_WORKER];
struct iotask *iotasks;
nvme_mutex_t lock[MAX_NUM_WORKER];
struct cmd_tasks cmd_buffer[MAX_NUM_WORKER];
nvme_mutex_t lock_master;
static int init_count;
static int io_count;
static unsigned io_issue_flag;

/* Function Definitions */

static uint64_t fetch_cmd(unsigned t){
	unsigned flag = 0;
	uint64_t result = 0;
	while (flag == 0){
		nvme_mutex_lock(&lock[t]);
		if (cmd_buffer[t].cnt != 0){
			result = cmd_buffer[t].head;
			cmd_buffer[t].head += 1;
			if (cmd_buffer[t].head == MAX_CMD_NUM)
				cmd_buffer[t].head = 0;
			cmd_buffer[t].cnt -= 1;
			flag = 1;
		}
		nvme_mutex_unlock(&lock[t]);
	}
	return result;
}

static int work_fn(void *arg){
	struct worker_thread *worker = (struct worker_thread *)arg;
	struct ns_worker_ctx *ns_ctx = worker->ns_ctx;
	struct perf_task *task = tasks[worker->lcore];
	int rc;

	ns_ctx->current_queue_depth = 0;

	printf("Starting thread on core %u\n", worker->lcore);

	if (nvme_register_io_thread() != 0) {
		fprintf(stderr, "nvme_register_io_thread() failed on core %u\n", worker->lcore);
		return -1;
	}
	
	task = rte_malloc(NULL, sizeof(struct perf_task), 0x200);
	task->buf = rte_malloc(NULL, f_maxsize*512*2, 0x200);	
	task->ns_ctx = ns_ctx;
	
	// Report slave's work begin.
	nvme_mutex_lock(&lock_master);
	init_count += 1;
	nvme_mutex_unlock(&lock_master);

	printf("[Debug] %u inited, with global %u/%u\n",
			worker->lcore, init_count, g_num_workers);

	while ((!io_issue_flag) || (cmd_buffer[worker->lcore].cnt)){
		uint64_t cmd_id = fetch_cmd(worker->lcore);
		if (ns_ctx->current_queue_depth > 0){
			nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
			while (ns_ctx->current_queue_depth > 0){
				nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
			}
		}
		uint64_t addr = iotasks[cmd_id].asu * f_maxlba + iotasks[cmd_id].lba;
		if (iotasks[cmd_id].type == 0){
			rc = submit_read(ns_ctx, task, addr, iotasks[cmd_id].size);		
			if (rc != 0){
				printf("Error at submiting command %lu\n", cmd_id);
				exit(1);
			}
		}	else{
			rc = submit_write(ns_ctx, task, addr, iotasks[cmd_id].size);		
			if (rc != 0){
				printf("Error at submiting command %lu\n", cmd_id);
				exit(1);
			}
		}
	}
	while (ns_ctx->current_queue_depth > 0){
		nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
	}
	
	// Report slave finished.
	nvme_mutex_lock(&lock_master);
	io_count += 1;
	nvme_mutex_unlock(&lock_master);

	printf("[Debug] %u finished, with global %u/%u\n",
			worker->lcore, io_count, g_num_workers);

	rte_free(task->buf);
	rte_free(task);
	nvme_unregister_io_thread();
	return 0;
}

static unsigned scheduler(uint64_t cmd){
	return ((cmd % g_num_workers) + 1);
}

static void update_cmd_queue(unsigned t, unsigned cmd){
	unsigned flag = 0;
	while (flag == 0){
		nvme_mutex_lock(&lock[t]);
		if (cmd_buffer[t].cnt != MAX_CMD_NUM){
			cmd_buffer[t].tail = cmd;
			cmd_buffer[t].tail += 1;
			if (cmd_buffer[t].tail == MAX_CMD_NUM)
				cmd_buffer[t].tail = 0;
			cmd_buffer[t].cnt += 1;
			flag = 1;
		}
		nvme_mutex_unlock(&lock[t]);
	}
}

static void master_fn(void){
	io_count = 0;
	io_issue_flag = 0;

	//Wait until all slave workers finish their init.
	while (init_count != g_num_workers);
	uint64_t tsc_start = rte_get_timer_cycles();
	for (uint64_t i = 0; i < f_len; i++){
		unsigned target = scheduler(i);
		update_cmd_queue(target, i);
		if (((i+1) % 100000) == 0)
			printf("Master has allocated %lu commands\n", i+1);
	}
	io_issue_flag = 1;
	while (io_count != g_num_workers);
	uint64_t tsc_end = rte_get_timer_cycles();
	double sec = (tsc_end - tsc_start) / (double)g_tsc_rate;
	printf("Time: %lf seconds\n", sec);
	printf("Throughput: %lf MB/S\n", (double)f_totalblocks/2048/sec);
}

int main(void){
	struct worker_thread *worker;
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

	init_count = 0;

	worker = g_workers->next;
	while (worker != NULL){
		rte_eal_remote_launch(work_fn, worker, worker->lcore);
		worker = worker->next;
	}
	
	//[TODO]Should insert the main admin function
	master_fn();

	worker = g_workers->next;
	while (worker != NULL){
		rte_eal_wait_lcore(worker->lcore);
		worker = worker->next;
	}

	//rc = work_fn(g_workers);
	free(iotasks);
	unregister_controllers();

}
