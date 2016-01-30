#include "base.h"

/* Variable Definitions */

struct rte_mempool *request_mempool = NULL;
struct ctrlr_entry *g_controllers = NULL;
struct ns_entry *g_namespaces = NULL;
int g_num_namespaces = 0;
struct worker_thread *g_workers = NULL;
struct worker_thread *g_master = NULL;
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

struct pending_tasks master_pending;
struct issue_struct issue_buf[MAX_NUM_WORKER];

int g_robin = -1;
uint64_t pending_count;

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
	for (int i = 0; i < QUEUE_NUM; i++){
		for (int j = 0; j < ISSUE_BUF_SIZE; j++){
			if (issue_buf[i].issue_queue[j].io_completed) continue;
			if (check_cover(issue_buf[i].issue_queue[j].cmd_id, cmd_id))
				conflict_flag = 1;	
		}
		if (conflict_flag) return 1;
	}
	return 0;
}

// Check whether this cmd(cmd_id)
// conflicts with either the ISSUE_BUF 
// or the PENDING_QUEUE. 
static unsigned check_conflict(uint64_t cmd_id){
	if (check_issue_conflict(cmd_id)) return 1;

	// Check pending queue first.
	struct pending_task *target = master_pending.head;
	while (target){
		if (check_cover(target->cmd_id, cmd_id))
			return 1;
		target = target->next;	
	}
	return 0;
	// Check issue_buf.
	//return check_issue_conflict(cmd_id);
}

static void clear_issue(int type){
	struct nvme_controller *ctrlr = g_master->ns_ctx->entry->u.nvme.ctrlr;

	if (type == -1){
		for (int i = 0; i < QUEUE_NUM; i++)
			if (g_master->queue_depth[i] > 0){
				//printf("Before %lu in queue %d\n", g_master->queue_depth[i], i); 
				nvme_ctrlr_process_io_completions_by_id(ctrlr, 0, i);
				//printf("After %lu in queue %d\n", g_master->queue_depth[i], i);
			}
	} else{
		if (g_master->queue_depth[type] > 0){
			//printf("Before %lu in queue %d\n", g_master->queue_depth[type], type);
			nvme_ctrlr_process_io_completions_by_id(ctrlr, 0, type);
			//printf("After %lu in queue %d\n", g_master->queue_depth[type], type);
		}
	}
}

// Select one worker to issue.
static int select_worker(void){	
	
	// Round-robin to the next one.
	int flag = 0;
	g_robin = (g_robin + 1) % QUEUE_NUM;
	
	// Chose the first one which meets the requirement
	// to issue.
	// Loop until we find one...
	while (true){
		// To ensure that in any time there 
		// is only at most ISSUE_BUF cmds in worker's workload.
		//clear_issue(g_robin);

		if ((g_master->queue_depth[g_robin]) < ISSUE_BUF_SIZE)
			flag = 1;
		
		// If above's worker is not able to receive...
		if (flag) return g_robin;
		clear_issue(g_robin);
		g_robin = (g_robin + 1) % QUEUE_NUM;
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
	
	if (check_conflict(cmd_id) == 1){
		if (master_pending.cnt == PENDING_QUEUE_SIZE) return -2;

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
		return -1;
	}
	
	return select_worker();
}

// Issue the command to the selected worker
static void master_issue(uint64_t cmd_id, int target){

	// Wait until we have space to issue
	while (g_master->queue_depth[target] == ISSUE_BUF_SIZE){
		clear_issue(target);	
	}

	// Issue directly
	for (int i = 0; i < ISSUE_BUF_SIZE; i++){
		if (issue_buf[target].issue_queue[i].io_completed == 1){
			// Write issue_buf slot
			issue_buf[target].issue_queue[i].io_completed = 0;
			issue_buf[target].issue_queue[i].cmd_id = cmd_id;		
			// Issue the cmd
			g_master->queue_depth[target] += 1;
			uint64_t addr = iotasks[cmd_id].asu * f_maxlba + iotasks[cmd_id].lba;
			// Distinguish the r/w
			struct issue_task *ptr = &(issue_buf[target].issue_queue[i]);
			if (iotasks[cmd_id].type == 0)
				submit_read(tasks[target], addr, iotasks[cmd_id].size, target, ptr);		
			else
				submit_write(tasks[target], addr, iotasks[cmd_id].size, target, ptr);
			break;
		}
	}


}
// Clear the pending queue.
static void clear_pending(void){
	if (master_pending.cnt == 0) return;
	int cnt = 0;	
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
		cnt += 1;
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
	}
}

// Master's main process.
// Each time we take care of the pending 
// queue first, then we focus on the new cmd
static void master_fn(void){

	nvme_register_io_thread();

	// Init pending task
	master_pending.head = NULL;
	master_pending.tail = NULL;
	master_pending.cnt = 0;
	
	// Init task buffer resource
	for (int i = 0; i < QUEUE_NUM; i++){
		tasks[i] = rte_malloc(NULL, sizeof(struct perf_task), 0x200);
		tasks[i]->buf = rte_malloc(NULL, (f_maxsize+1000)*512, 0x200);	
		for (int j = 0; j < ISSUE_BUF_SIZE; j++){
			issue_buf[i].issue_queue[j].io_completed = 1;
			issue_buf[i].issue_queue[j].qid = i;
		}
	}

	//Begin timing.
	uint64_t tsc_start = rte_get_timer_cycles();
	
	uint64_t pos = 0;
	while (pos < f_len){
		//printf("%lu\n", pos);
		clear_issue(-1);		
		clear_pending();
		int target = scheduler(pos);
		if (target >= 0)
		  master_issue(pos, target);
	
		if (target != -2){
			pos += 1;	
			if ((pos % 100000) == 0)
				printf("Master has (allocated && (issued || pending)) %lu commands\n", pos);
		}
	
	}
	printf("Master has issued all of the I/O commands\n");
	
	// Clear all the pending instruction
	while (master_pending.cnt != 0) {
		clear_issue(-1);
		clear_pending();
	}
	
	// Check out all the issued commands
	int flag = 1;
	while (flag){
		flag = 0;
		clear_issue(-1);
		for (int i = 0; i < QUEUE_NUM; i++){
			if (g_master->queue_depth[i]) flag = 1;
		}
	}

	//Stop timing.
	uint64_t tsc_end = rte_get_timer_cycles();
	
	//Get the total time.
	double sec = (tsc_end - tsc_start) / (double)g_tsc_rate;
	
	printf("Stat of pending count: %lu\n", pending_count);

	//Output the result infomation.
	printf("Time: %lf seconds\n", sec);
	printf("Throughput: %lf MB/S\n", (double)f_totalblocks/2048/sec);
	printf("IOPS: %lf /S\n", (double)f_len/sec);
	for (int i = 0; i < QUEUE_NUM; i++){
		rte_free(tasks[i]->buf);
		rte_free(tasks[i]);
	}

	nvme_unregister_io_thread();
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
	
	// Init task buffer resource
	master_fn();
	free(iotasks);
	unregister_controllers();

}
