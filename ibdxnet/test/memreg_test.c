#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <malloc.h>

#include <infiniband/verbs.h>

int main(int argc, char** argv)
{
	int mem_reg_size;
	int count;

	struct ibv_device** dev_list;
	struct ibv_context* ib_ctx;
	struct ibv_pd* prot_dom;
	struct ibv_device_attr device_attr;

	if (argc < 3) {
		printf("Usage: %s <mem reg size> <count>\n", argv[0]);
		return -1;
	}

	mem_reg_size = atoi(argv[1]);
	count = atoi(argv[2]);


	dev_list = ibv_get_device_list(NULL);
	ib_ctx = ibv_open_device(dev_list[0]);

	ibv_query_device(ib_ctx, &device_attr);
	printf("device max_mr_size: %d\n", device_attr.max_mr_size);	

	prot_dom = ibv_alloc_pd(ib_ctx);

	void** mem_regs = malloc(sizeof(void*) * count);

	for (int i = 0; i < count; i++) {
		// TODO alloc memalign
		mem_regs[i] = memalign(sysconf(_SC_PAGESIZE), mem_reg_size);
		//mem_regs[i] = malloc(mem_reg_size);
	}
	
	for (int i = 0; i < count; i++) {
		struct ibv_mr* mem_reg = ibv_reg_mr(prot_dom, mem_regs[i], mem_reg_size, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
		
		if (!mem_reg) {
			printf("ERROR allocation memory region %d: %s\n", i, strerror(errno));
			return -1;
		}
	}

	printf("Success\n");
	
 	ibv_dealloc_pd(prot_dom);
    ibv_close_device(ib_ctx);

	return 0;
}
