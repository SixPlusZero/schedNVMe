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

	while ((!io_issue_flag) || (master_pending.cnt)){
		while (ns_ctx->current_queue_depth > 0)
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

static unsigned check_cover(uint64_t cmd_a, uint64_t cmd_b){
	struct iotask *c1 = &iotasks[cmd_a];
	struct iotask *c2 = &iotasks[cmd_b];
	
	uint64_t lba1 = c1->asu * f_maxlba + c1->lba;
	uint64_t lba2 = c2->asu * f_maxlba + c2->lba;

	if ((lba1 + c1->size - 1) < lba2) return 0;
	if ((lba2 + c2->size - 1) < lba1) return 0;
	
	if ((!c1->type) && (!c2->type)) return 0;
	
	return 1;	
}

static unsigned check_conflict(uint64_t cmd_id){
	unsigned conflict_flag = 0;
	unsigned pos = master_pending->head;
	for (unsigned i = 0; i < master_pending->cnt; i++){
		if (check_cover(master_pending[pos], cmd))
			return 1;		
	}
	for (int i = 1; i <= g_num_workers; i++){
		nvme_mutex_lock(&lock[i]);
		for (int j = 0; j < ISSUE_BUF_SIZE; j++){
			if (issue_buf[i].issue_queue[j].io_completed) continue;
			if (check_cover(issue_buf[i].issue_queue[j].cmd_id, cmd)
				conflict_flag = 1;	
		}
		nvme_mutex_unlock(&lock[i]);
		if (conflict_flag) return 1;
	}
	return 0;
}

static int select_worker(void){	
	int flag = 0;
	g_robin += 1;
	if (g_robin > g_num_workers) 
		g_robin = 1;
	
	while (true){
		nvme_mutex_lock(&lock[g_robin]);
		if (issue_buf[g_robin].ctx->current_queue_depth != ISSUE_BUF_SIZE)
			flag = 1;
		nvme_mutex_unlock(&lock[g_robin]);
		if (flag) return g_robin;
		g_robin += 1;
		if (g_robin > g_num_workers)
			g_robin = 1;
	}
}

static int scheduler(uint64_t cmd_id){
	if (check_conflict(cmd_id)){
		if (master_pending.cnt == PENDING_QUEUE_SIZE)
			return -1;
		master_pending.pending_queue[master_pending.cnt] = cmd_id;
		master_pending.cnt += 1;
		master_pending.tail += 1;
		if (master_pending.tail == PENDING_QUEUE_SIZE) 
			master_pending.tail = 0;
		return 0;
	}
	return select_worker();
}

static void master_issue(uint64_t cmd_id, int target){
	nvme_mutex_lock(&lock[target]);
	for (unsigned i = 0; i < ISSUE_BUF_SIZE; i++){
		if (issue_buf[target].issue_queue[i].io_completed == 1){
			issue_buf[target].issue_queue[i].io_completed = 0;
			issue_buf[target].issue_queue[i].cmd_id = cmd_id;
			break;
		}
	}
	nvme_mutex_unlock(&lock[target]);

	uint64_t addr = iotasks[cmd_id].asu * f_maxlba + iotasks[cmd_id].lba;
	if (iotasks[cmd_id].type == 0){
		rc = submit_read(target, task, addr, iotasks[cmd_id].size);		
		if (rc != 0){
			printf("Error at submiting command %lu\n", cmd_id);
			exit(1);
		}
	}	else{
		rc = submit_write(target, task, addr, iotasks[cmd_id].size);		
		if (rc != 0){
			printf("Error at submiting command %lu\n", cmd_id);
			exit(1);
		}
	}
}

static void clear_pending(void){
	if (master_pending.cnt == 0) return;
	while (true){
		uint64_t cmd_id = master_pending.pending_queue[master_pending.head].cmd_id;
		if (check_issue_conflict(cmd_id)) return;	
		int rc = select_worker();
		master_issue(cmd_id, rc);
		master_pending.cnt -= 1;
		master_pending.head += 1;
		if (master_pending.head == PENDING_QUEUE_SIZE)
			master_pending.head = 0;
		if (master_pending.cnt == 0)
			return;
	}	
}

static void master_fn(void){
	io_count = 0;
	io_issue_flag = 0;

	//Wait until all slave workers finish their init.
	while (init_count != g_num_workers);
	
	//Begin timing.
	uint64_t tsc_start = rte_get_timer_cycles();
	
	uint64_t pos = 0;
	while (pos < f_len){
		clear_pending();
		int target = scheduler(pos);
		if (target > 0){
			master_issue(pos, target);
		}
		if (target != -1){
			pos += 1;	
			if ((pos % 100000) == 0)
				printf("Master has (allocated && issued) %lu commands\n", pos);
		}
	}
	
	//Tag that master has allocated all the commands.
	io_issue_flag = 1;
	
	while (io_count != g_num_workers);
	
	//Stop timing.
	uint64_t tsc_end = rte_get_timer_cycles();
	
	//Get the total time.
	double sec = (tsc_end - tsc_start) / (double)g_tsc_rate;
	
	//Output the result infomation.
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
