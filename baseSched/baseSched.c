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
nvme_mutex_t lock_master;

static int init_count;
static int io_count;
static unsigned io_issue_flag;

struct pending_tasks master_pending;
struct issue_struct issue_buf[MAX_NUM_WORKER];
struct cmd_struct cmd_buf[MAX_NUM_WORKER];

int g_robin;
uint64_t pending_count;

/* Function Definitions */

// Fetch one command from its cmd_buf
// to its issue_buf
static struct submit_struct fetch_cmd(struct ns_worker_ctx *ns_ctx, unsigned target){
	struct submit_struct result;
	uint64_t cmd_id;


	nvme_mutex_lock(&lock[target]);
	
	// cmd_buf->worker
	cmd_id = cmd_buf[target].queue[cmd_buf[target].head];
	cmd_buf[target].head += 1;
	if (cmd_buf[target].head == CMD_BUF_SIZE) cmd_buf[target].head = 0;
	cmd_buf[target].cnt -= 1;
	// worker->issue_buf
	for (unsigned i = 0; i < ISSUE_BUF_SIZE; i++){
		if (issue_buf[target].issue_queue[i].io_completed == 1){
			issue_buf[target].issue_queue[i].io_completed = 0;
			issue_buf[target].issue_queue[i].cmd_id = cmd_id;
			result.cmd_id = cmd_id;
			result.ptr = (void *) &(issue_buf[target].issue_queue[i]);
			//printf("Show %lu, %lu\n", result.cmd_id, ((struct issue_task *)(result.ptr))->cmd_id);
			break;
		}
	}
	nvme_mutex_unlock(&lock[target]);
	// Increase the sq depth here to ensure the consistency.
	ns_ctx->current_queue_depth += 1;

	return result;
}

// Worker's main
static int work_fn(void *arg){
	struct worker_thread *worker = (struct worker_thread *)arg;
	struct ns_worker_ctx *ns_ctx = worker->ns_ctx;
	struct perf_task *task = tasks[worker->lcore];
	printf("task at core %u\n", worker->lcore);
	ns_ctx->current_queue_depth = 0;

	printf("Starting thread on core %u\n", worker->lcore);

	if (nvme_register_io_thread() != 0) {
		fprintf(stderr, "nvme_register_io_thread() failed on core %u\n", worker->lcore);
		return -1;
	}
	
	task = rte_malloc(NULL, sizeof(struct perf_task), 0x200);
	task->buf = rte_malloc(NULL, f_maxsize*512*2, 0x200);	
	task->ns_ctx = ns_ctx;
	for (unsigned i = 0; i < ISSUE_BUF_SIZE; i++){
		issue_buf[worker->lcore].issue_queue[i].ns_ctx = ns_ctx;
		issue_buf[worker->lcore].issue_queue[i].io_completed = 1;
	}
	// Report slave's work begin.
	nvme_mutex_lock(&lock_master);
	init_count += 1;
	printf("[Debug] %u inited, with global %u/%u\n",
			worker->lcore, init_count, g_num_workers);
	nvme_mutex_unlock(&lock_master);

	// Master is not ready.
	while (init_count != g_num_workers);
	
	// Loop until master has sent all the commands
	// to all the slaves.
	while ((!io_issue_flag) || (master_pending.cnt)){
		
		// cnt > 0 means we still have cmds to do.
		if (cmd_buf[worker->lcore].cnt){
			while (ns_ctx->current_queue_depth == ISSUE_BUF_SIZE);
			// Fetch cmd_id
			struct submit_struct r = fetch_cmd(ns_ctx, worker->lcore);
			uint64_t cmd_id = r.cmd_id;

			// Issue the cmd
			uint64_t addr = iotasks[cmd_id].asu * f_maxlba + iotasks[cmd_id].lba;
			// Distinguish the r/w
			if (iotasks[cmd_id].type == 0)
				submit_read(ns_ctx, worker->lcore, task, 
						addr, iotasks[cmd_id].size, r.cmd_id, r.ptr);		
			else
				submit_write(ns_ctx, worker->lcore, task, 
						addr, iotasks[cmd_id].size, r.cmd_id, r.ptr);		
			
		}
		// Here we could not use while to block the process.
		if (ns_ctx->current_queue_depth > 0)
			nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);

	}

	// Do the clean work.
	while (ns_ctx->current_queue_depth > 0)
		nvme_ctrlr_process_io_completions(ns_ctx->entry->u.nvme.ctrlr, 0);
	
	// Report to master that slave's work has finished.
	nvme_mutex_lock(&lock_master);
	io_count += 1;
	printf("[Write Count] %lu\n", ns_ctx->write_cnt);
	printf("[IO Completed] %lu\n", ns_ctx->io_completed);
	printf("[Debug] %u finished, with global %u/%u\n",
			worker->lcore, io_count, g_num_workers);
	nvme_mutex_unlock(&lock_master);

	rte_free(task->buf);
	rte_free(task);
	nvme_unregister_io_thread();
	return 0;
}

// Check whether two cmds have both
// the addr overlap and the type conflict.
static inline unsigned check_cover(uint64_t cmd_a, uint64_t cmd_b){
	struct iotask *c1 = &iotasks[cmd_a];
	struct iotask *c2 = &iotasks[cmd_b];
	
	uint64_t lba1 = c1->asu * f_maxlba + c1->lba;
	uint64_t lba2 = c2->asu * f_maxlba + c2->lba;

	if ((lba1 + c1->size - 1) < lba2) return 0;
	if ((lba2 + c2->size - 1) < lba1) return 0;
	
	if ((!c1->type) && (!c2->type)) return 0;
	
	return 1;	
}

// Check whether this cmd(cmd_id) 
// conflicts with either of all the ISSUED cmds.
static unsigned check_issue_conflict(uint64_t cmd_id){
	unsigned conflict_flag = 0;

	for (int i = 1; i <= g_num_workers; i++){
		nvme_mutex_lock(&lock[i]);
	
		for (int j = 0; j < ISSUE_BUF_SIZE; j++){
			if (issue_buf[i].issue_queue[j].io_completed) continue;
			if (check_cover(issue_buf[i].issue_queue[j].cmd_id, cmd_id))
				conflict_flag = 1;	
		}
		
		unsigned pos = cmd_buf[i].head;
		for (unsigned j = 0; j < cmd_buf[i].cnt; j++){
			if (check_cover(cmd_buf[i].queue[pos], cmd_id)){
				conflict_flag = 1;
				break;
			}
			pos += 1;
			if (pos == CMD_BUF_SIZE) pos = 0;
		}
	
		nvme_mutex_unlock(&lock[i]);

		if (conflict_flag) return 1;
	}
	return 0;
}

// Check whether this cmd(cmd_id)
// conflicts with either the ISSUE_BUF 
// or the PENDING_QUEUE. 
static inline unsigned check_conflict(uint64_t cmd_id){
	//return 0;	
	// Check pending queue first.
	struct pending_task *target = master_pending.head;
	while (target){
		if (check_cover(target->cmd_id, cmd_id))
			return 1;
		target = target->next;	
	}

	// Check issue_buf.
	return check_issue_conflict(cmd_id);
}

// Select one worker to issue.
static int select_worker(void){	
	
	// Round-robin to the next one.
	int flag = 0;
	g_robin += 1;
	if (g_robin > g_num_workers) g_robin = 1;
	
	// Chose the first one which meets the requirement
	// to issue.
	// Loop until we find one...
	while (true){
		//nvme_mutex_lock(&lock[g_robin]);
		
		// To ensure that in any time there 
		// is only at most ISSUE_BUF cmds in worker's workload.
		if ((issue_buf[g_robin].ctx->current_queue_depth + cmd_buf[g_robin].cnt) < ISSUE_BUF_SIZE - 2)
			flag = 1;
		//nvme_mutex_unlock(&lock[g_robin]);
		
		// If above's worker is not able to receive...
		if (flag) return g_robin;
		g_robin += 1;
		if (g_robin > g_num_workers) g_robin = 1;
	}
}

// The master's scheduler.
// It behaves as follows:
// First, check conflicts withe previous cmds:
// -- If any, look at the pending queue:
// 		-- If FULL, return -1.
// 		-- Else, return 0;
// -- If none, call the select_worker() to return the worker id.
static int scheduler(uint64_t cmd_id){
	
	if (check_conflict(cmd_id)){
		if (master_pending.cnt == PENDING_QUEUE_SIZE) return -1;

		if (master_pending.tail == NULL){
		 	master_pending.tail = malloc(sizeof(struct pending_task));
			master_pending.head = master_pending.tail;
			master_pending.tail->prev = NULL;
			master_pending.tail->next = NULL;
			master_pending.cnt = 1;
		} else{
			master_pending.cnt += 1;
			master_pending.tail->next = malloc(sizeof(struct pending_task));
			master_pending.tail->next->next = NULL;
			master_pending.tail->next->prev = master_pending.tail;
			master_pending.tail = master_pending.tail->next;
		}
		master_pending.tail->cmd_id = cmd_id;
		
		pending_count += 1;
		return 0;
	}
	
	return select_worker();
}

// Issue the command to the selected worker
static void master_issue(uint64_t cmd_id, int target){

	// Wait the cmd_buf ready to write
	// Logically this could not happen because
	// when we reach here the issue cmd number are less than ten.
	while (cmd_buf[target].cnt == CMD_BUF_SIZE);
	
	nvme_mutex_lock(&lock[target]);
	
	// Write the slave worker's cmd_buf. 
	cmd_buf[target].queue[cmd_buf[target].tail] = cmd_id;
	cmd_buf[target].tail += 1;
	if (cmd_buf[target].tail == CMD_BUF_SIZE)
		cmd_buf[target].tail = 0;
	cmd_buf[target].cnt += 1;

	nvme_mutex_unlock(&lock[target]);
}

// Clear the pending queue.
static void clear_pending(void){
	if (master_pending.cnt == 0) return;
	
	struct pending_task *target = master_pending.head;

	while (target){
		int cflag = 0;
		uint64_t cmd_id = master_pending.head->cmd_id;
		
		// If head cmd still cannot issue, just return
		if (check_issue_conflict(cmd_id)) cflag = 1;	
		
		if (!cflag){
			struct pending_task *cp_ptr = master_pending.head;
			while (cp_ptr != target){
				if (check_cover(cp_ptr->cmd_id, cmd_id)){
					cflag = 1;
					break;
				}
				cp_ptr = cp_ptr->next;
			}
		}
		if (cflag){
			target = target -> next;
			continue;
		}
		
		// Here we issue first so that
		// the worker would receive all the cmds from the buf.
		int rc = select_worker();
		master_issue(cmd_id, rc);		

		master_pending.cnt -= 1;	
		struct pending_task *back_target = target->next;
		if (target->prev) target->prev->next = target->next;
		if (target->next) target->next->prev = target->prev;
		if (target == master_pending.head) master_pending.head = target->next;
		if (target == master_pending.tail) master_pending.tail = target->prev;
		free(target);
		target = back_target;
		//if (master_pending.cnt == 0) break;
	}	
}

// Master's main process.
// Each time we take care of the pending 
// queue first, then we focus on the new cmd
static void master_fn(void){
	io_count = 0;
	io_issue_flag = 0;
	
	// Init pending task
	master_pending.head = NULL;
	master_pending.tail = NULL;
	master_pending.cnt = 0;


	//Wait until all slave workers finish their init.
	while (init_count != g_num_workers);
	
	//Begin timing.
	uint64_t tsc_start = rte_get_timer_cycles();
	printf("All slaves are ready now\n");
	sleep(1);
	
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
				printf("Master has (allocated && (issued || pending)) %lu commands\n", pos);
		}
	}

	// Clear all the pending instruction
	while (master_pending.cnt != 0) clear_pending();
	
	//Tag that master has allocated all the commands.
	io_issue_flag = 1;
	
	while (io_count != g_num_workers);
	
	//Stop timing.
	uint64_t tsc_end = rte_get_timer_cycles();
	
	//Get the total time.
	double sec = (tsc_end - tsc_start) / (double)g_tsc_rate;
	
	printf("Stat of pending count: %lu\n", pending_count);

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
	
	master_fn();

	worker = g_workers->next;
	while (worker != NULL){
		rte_eal_wait_lcore(worker->lcore);
		worker = worker->next;
	}

	free(iotasks);
	unregister_controllers();

}
