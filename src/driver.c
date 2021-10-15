/*
 * driver.c
 * driver for the tpcc transactions
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/times.h>
#include <time.h>
#include "tpc.h"      /* prototypes for misc. functions */
#include "trans_if.h" /* prototypes for transacation interface calls */
#include "sequence.h"
#include "rthist.h"
#include "sb_percentile.h"

static int other_ware (int home_ware);
static int do_neword (int t_num);
static int do_payment (int t_num);
static int do_ordstat (int t_num);
static int do_delivery (int t_num);
static int do_slev (int t_num);

extern int num_ware;
extern int num_conn;
extern int activate_transaction;
extern int counting_on;

extern int num_node;
extern int time_count;
extern FILE *freport_file;

extern int use_wait_time;
extern int num_driver;
extern int driver_id;

extern int success[];
extern int late[];
extern int retry[];
extern int failure[];

extern int* success2[];
extern int* late2[];
extern int* retry2[];
extern int* failure2[];

extern double max_rt[];
extern double total_rt[];

extern int rt_limit[];

extern long clk_tck;
extern sb_percentile_t local_percentile;

#define KEY_TIME_NEWORD   18000
#define KEY_TIME_PAYMENT  3000
#define KEY_TIME_ORDSTAT  2000
#define KEY_TIME_DELIVERY 2000
#define KEY_TIME_SLEV     2000

int key_time[5] = {
    KEY_TIME_NEWORD,
    KEY_TIME_PAYMENT,
    KEY_TIME_ORDSTAT,
    KEY_TIME_DELIVERY,
    KEY_TIME_SLEV
};

#define MEAN_THINK_TIME_NEWORD   12000
#define MEAN_THINK_TIME_PAYMENT  12000
#define MEAN_THINK_TIME_ORDSTAT  10000
#define MEAN_THINK_TIME_DELIVERY 5000
#define MEAN_THINK_TIME_SLEV     5000

int mean_think_time[5] = {
    MEAN_THINK_TIME_NEWORD,
    MEAN_THINK_TIME_PAYMENT,
    MEAN_THINK_TIME_ORDSTAT,
    MEAN_THINK_TIME_DELIVERY,
    MEAN_THINK_TIME_SLEV
};

#define MAX_RETRY 2000
#define NUM_TERMINALS 10

int driver (int t_num)
{
    int i, j;
    int tx_type;
    double think_time_factor;
    clock_t clk;
    struct timespec tbuf;
    double current_time;
    /* The time where the next transaction can be executed for each terminal. */
    double wait_until[NUM_TERMINALS];
    int next_tx_type[NUM_TERMINALS];
    int terminal;
    time_t start_sec;

    if(use_wait_time){
      clk = clock_gettime(CLOCK_MONOTONIC, &tbuf);
      start_sec = tbuf.tv_sec;
      current_time = (tbuf.tv_sec - start_sec) * 1000.0 + tbuf.tv_nsec / 1000000.0;

      for(i = 0; i < NUM_TERMINALS; i++){
        next_tx_type[i] = seq_get();
        wait_until[i] = current_time + key_time[next_tx_type[i]];
      }
    }

    while( activate_transaction ){

      if(use_wait_time){
        clk = clock_gettime(CLOCK_MONOTONIC, &tbuf);
        current_time = (tbuf.tv_sec - start_sec) * 1000.0 + tbuf.tv_nsec / 1000000.0;
        /* Find the terminal with the earliest time to execute. */
        terminal = -1;
        for(i = 0; i < NUM_TERMINALS; i++)
        {
            if(terminal == -1 || wait_until[i] < wait_until[terminal])
            {
                terminal = i;
            }
        }
        if (wait_until[terminal] > current_time)
        {
            /* Sleep if no terminal has transaction to execute yet. */
            usleep((long long)(1000 * (wait_until[terminal] - current_time)) + 1);
            continue;
        }
        else
        {
            tx_type = next_tx_type[terminal];
        }
      }
      else{
        tx_type = seq_get();
      }

      switch(tx_type){
      case 0:
        do_neword(t_num);
        break;
      case 1:
        do_payment(t_num);
        break;
      case 2:
        do_ordstat(t_num);
        break;
      case 3:
        do_delivery(t_num);
        break;
      case 4:
        do_slev(t_num);
        break;
      default:
        printf("Error - Unknown sequence.\n");
      }

      if(use_wait_time){
        think_time_factor = -log((rand() + 0.5) / RAND_MAX);
        if(think_time_factor > 10){
          /* According to TPCC spec, each distribution may be truncated at 10 times its mean. */
          think_time_factor = 10;
        }

        clk = clock_gettime(CLOCK_MONOTONIC, &tbuf);
        current_time = (tbuf.tv_sec - start_sec) * 1000.0 + tbuf.tv_nsec / 1000000.0;
        next_tx_type[terminal] = seq_get();
        wait_until[terminal] = current_time
            + think_time_factor * mean_think_time[tx_type]
            + key_time[next_tx_type[terminal]];
      }
    }

    return(0);

}

/*
 * get the warehouse id corresponds to the current connection.
 */
static int get_warehouse(int t_num)
{
    int c_num, w_id;

    if(use_wait_time){
      w_id = driver_id + 1 + num_driver * t_num;
    }
    else if(num_node==0){
      w_id = RandomNumber(1, num_ware);
    }else{
      c_num = ((num_node * t_num)/num_conn); /* drop moduls */
      w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
                          (num_ware * (c_num + 1))/num_node);
    }
    return w_id;
}

/*
 * prepare data and execute the new order transaction for one order
 * officially, this is supposed to be simulated terminal I/O
 */
static int do_neword (int t_num)
{
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id, ol_cnt;
    int  all_local = 1;
    int  notfound = MAXITEMS+1;  /* valid item ids are numbered consecutively
				    [1..MAXITEMS] */
    int rbk;
    int  itemid[MAX_NUM_ITEMS];
    int  supware[MAX_NUM_ITEMS];
    int  qty[MAX_NUM_ITEMS];

    w_id = get_warehouse(t_num);
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST);

    ol_cnt = RandomNumber(5, 15);
    rbk = RandomNumber(1, 100);

    for (i = 0; i < ol_cnt; i++) {
	itemid[i] = NURand(8191, 1, MAXITEMS);
	if ((i == ol_cnt - 1) && (rbk == 1)) {
	    itemid[i] = notfound;
	}
	if (RandomNumber(1, 100) != 1) {
	    supware[i] = w_id;
	}
	else {
	    supware[i] = other_ware(w_id);
	    all_local = 0;
	}
	qty[i] = RandomNumber(1, 10);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = neword(t_num, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
        //printf("NOT : %.3f\n", rt);
        if (freport_file != NULL) {
          fprintf(freport_file,"%d %.3f\n", time_count, rt);
        }

	if(rt > max_rt[0])
	  max_rt[0]=rt;
	total_rt[0] += rt;
	sb_percentile_update(&local_percentile, rt);
	hist_inc(0, rt);
	if(counting_on){
	  if( rt < rt_limit[0]){
	    success[0]++;
	    success2[0][t_num]++;
	  }else{
	    late[0]++;
	    late2[0][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[0]++;
	  retry2[0][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[0]--;
      retry2[0][t_num]--;
      failure[0]++;
      failure2[0][t_num]++;
    }

    return (0);
}

/*
 * produce the id of a valid warehouse other than home_ware
 * (assuming there is one)
 */
static int other_ware (int home_ware)
{
    int tmp;

    if (num_ware == 1) return home_ware;
    while ((tmp = RandomNumber(1, num_ware)) == home_ware);
    return tmp;
}

/*
 * prepare data and execute payment transaction
 */
static int do_payment (int t_num)
{
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
    char c_last[17];

    w_id = get_warehouse(t_num);
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    h_amount = RandomNumber(1,5000);
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }
    if (RandomNumber(1, 100) <= 85) {
        c_w_id = w_id;
        c_d_id = d_id;
    }else{
        c_w_id = other_ware(w_id);
        c_d_id = RandomNumber(1, DIST_PER_WARE);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = payment(t_num, w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[1])
	  max_rt[1]=rt;
	total_rt[1] += rt;
	hist_inc(1, rt);
	if(counting_on){
	  if( rt < rt_limit[1]){
	    success[1]++;
	    success2[1][t_num]++;
	  }else{
	    late[1]++;
	    late2[1][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[1]++;
	  retry2[1][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[1]--;
      retry2[1][t_num]--;
      failure[1]++;
      failure2[1][t_num]++;
    }

    return (0);
}

/*
 * prepare data and execute order status transaction
 */
static int do_ordstat (int t_num)
{
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id;
    char c_last[16];

    w_id = get_warehouse(t_num);
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = ordstat(t_num, w_id, d_id, byname, c_id, c_last);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[2])
	  max_rt[2]=rt;
	total_rt[2] += rt;
	hist_inc(2, rt);
	if(counting_on){
	  if( rt < rt_limit[2]){
	    success[2]++;
	    success2[2][t_num]++;
	  }else{
	    late[2]++;
	    late2[2][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[2]++;
	  retry2[2][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[2]--;
      retry2[2][t_num]--;
      failure[2]++;
      failure2[2][t_num]++;
    }

    return (0);

}

/*
 * execute delivery transaction
 */
static int do_delivery (int t_num)
{
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, o_carrier_id;

    w_id = get_warehouse(t_num);
    o_carrier_id = RandomNumber(1, 10);

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = delivery(t_num, w_id, o_carrier_id);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[3])
	  max_rt[3]=rt;
	total_rt[3] += rt;
	hist_inc(3, rt );
	if(counting_on){
	  if( rt < rt_limit[3]){
	    success[3]++;
	    success2[3][t_num]++;
	  }else{
	    late[3]++;
	    late2[3][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[3]++;
	  retry2[3][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[3]--;
      retry2[3][t_num]--;
      failure[3]++;
      failure2[3][t_num]++;
    }

    return (0);

}

/* 
 * prepare data and execute the stock level transaction
 */
static int do_slev (int t_num)
{
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, level;

    w_id = get_warehouse(t_num);
    d_id = RandomNumber(1, DIST_PER_WARE); 
    level = RandomNumber(10, 20); 

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = slev(t_num, w_id, d_id, level);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[4])
	  max_rt[4]=rt;
	total_rt[4] += rt;
	hist_inc(4, rt );
	if(counting_on){
	  if( rt < rt_limit[4]){
	    success[4]++;
	    success2[4][t_num]++;
	  }else{
	    late[4]++;
	    late2[4][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[4]++;
	  retry2[4][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[4]--;
      retry2[4][t_num]--;
      failure[4]++;
      failure2[4][t_num]++;
    }

    return (0);

}
