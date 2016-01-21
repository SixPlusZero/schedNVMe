
SPDK_ROOT_DIR := $(CURDIR)/../../..
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

APP = baseSched

C_SRCS := baseSched.c

CFLAGS += -I. $(DPDK_INC)

SPDK_LIBS += $(SPDK_ROOT_DIR)/lib/nvme/libspdk_nvme.a \
	     $(SPDK_ROOT_DIR)/lib/util/libspdk_util.a \
	     $(SPDK_ROOT_DIR)/lib/memory/libspdk_memory.a

LIBS += $(SPDK_LIBS) -lpciaccess -lpthread $(DPDK_LIB) -lrt

ifeq ($(OS),Linux)
LIBS += -laio
CFLAGS += -DHAVE_LIBAIO
endif

all : $(APP)

$(APP) : $(OBJS) $(SPDK_LIBS)
	$(LINK_C)

clean :
	$(Q)rm -f $(OBJS) *.d $(APP)

include $(SPDK_ROOT_DIR)/mk/spdk.deps.mk
