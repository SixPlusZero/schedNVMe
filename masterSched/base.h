#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdint.h>

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

typedef unsigned long int   uint64_t;
#define MAX_NUM_WORKER 16
#define ISSUE_BUF_SIZE 50
#define PENDING_QUEUE_SIZE 1000
#define QUEUE_NUM 8


struct ctrlr_entry {
	struct nvme_controller	*ctrlr;
	struct ctrlr_entry	*next;
	char name[1024];
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
	uint64_t io_completed;
	uint64_t current_queue_depth;
	struct ns_worker_ctx *next;
};

struct perf_task {
	struct ns_worker_ctx	*ns_ctx;
	void	*buf;
	unsigned arg1;
	unsigned arg2;
};

struct worker_thread {
	struct ns_worker_ctx 	*ns_ctx;
	struct worker_thread	*next;
	uint64_t queue_depth[32];
	unsigned lcore;
};

struct issue_task{
	uint64_t cmd_id;
	uint64_t io_completed; // Whether it's completed.
	int qid;
};

struct issue_struct{
	struct issue_task issue_queue[ISSUE_BUF_SIZE];
};

struct iotask {
	uint32_t lba;
	uint32_t size;
	uint32_t asu;
	uint32_t type;
};

struct pending_task{
	uint64_t cmd_id;
	struct pending_task *prev;
	struct pending_task *next;
};

struct pending_tasks{
	struct pending_task *head;
	struct pending_task *tail;
	unsigned cnt;
};

/* Variable Declaration */
extern struct rte_mempool *request_mempool;
extern struct ctrlr_entry *g_controllers;
extern struct ns_entry *g_namespaces;
extern int g_num_namespaces;
extern struct worker_thread *g_workers;
extern struct worker_thread *g_master;
extern int g_num_workers;
extern uint64_t g_tsc_rate;
extern int g_io_size_bytes;
extern int g_time_in_sec;
extern uint32_t g_max_completions;
extern const char *g_core_mask;
extern uint64_t f_len;
extern uint64_t f_maxasu;
extern uint64_t f_maxlba;
extern uint64_t f_maxsize;
extern uint64_t f_totalblocks;
extern uint64_t iotask_read_count;
extern uint64_t iotask_write_count;
extern char *ealargs[];
extern struct iotask *iotasks;
extern struct perf_task *tasks[MAX_NUM_WORKER];
extern nvme_mutex_t lock[MAX_NUM_WORKER];
extern nvme_mutex_t lock_master;
extern struct pending_tasks master_pending;
extern struct issue_struct issue_buf[MAX_NUM_WORKER];
extern int g_robin;



/* Function Declaration */
extern int initSPDK(void);
extern int initTrace(void);
extern void register_ns(struct nvme_controller *ctrlr, struct pci_device *pci_dev, struct nvme_namespace *ns);
extern void register_ctrlr(struct nvme_controller *ctrlr, struct pci_device *pci_dev);
extern void task_ctor(struct rte_mempool *mp, void *arg, void *__task, unsigned id);
extern int register_workers(void);
extern int register_controllers(void);
extern void unregister_controllers(void);
extern int associate_workers_with_ns(void);
extern int initSPDK(void);
extern int initTrace(void);
extern int replay_split(struct iotask *dst, char* str);
extern void task_complete(struct issue_task *task);
extern void io_complete(void *ctx, const struct nvme_completion *completion);
extern int submit_read(struct perf_task *task, uint64_t lba, uint64_t num_blocks, int arg1, void *arg2);
extern int submit_write(struct perf_task *task, uint64_t lba, uint64_t num_blocks, int arg1, void *arg2);
