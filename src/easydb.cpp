/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * easydb.cpp
 *
 * Identification: src/easydb.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include "analyze/analyze.h"
#include "common/errors.h"
#include "common/portal.h"
#include "optimizer/optimizer.h"
#include "planner/plan.h"
#include "planner/planner.h"
#include "recovery/log_recovery.h"

// #define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8
using namespace easydb;

int SOCK_PORT = 8765;

static bool should_exit = false;
bool for_web = false;

std::unique_ptr<DiskManager> disk_manager;
std::unique_ptr<BufferPoolManager> buffer_pool_manager;
std::unique_ptr<RmManager> rm_manager;
std::unique_ptr<IxManager> ix_manager;
std::unique_ptr<SmManager> sm_manager;
std::unique_ptr<LockManager> lock_manager;
std::unique_ptr<TransactionManager> txn_manager;
std::unique_ptr<Planner> planner;
std::unique_ptr<Optimizer> optimizer;
std::unique_ptr<QlManager> ql_manager;
std::unique_ptr<LogManager> log_manager;
std::unique_ptr<RecoveryManager> recovery;
std::unique_ptr<Analyze> analyze;
std::unique_ptr<Portal> portal;

int sockfd;
pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
  should_exit = true;
  log_manager->flush_log_to_disk();
  std::cout << "The Server receive Crtl+C, will been closed\n";
  longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t *txn_id, Context *context) {
  context->txn_ = txn_manager->GetTransaction(*txn_id);
  if (context->txn_ == nullptr || context->txn_->GetState() == TransactionState::COMMITTED ||
      context->txn_->GetState() == TransactionState::ABORTED) {
    context->txn_ = txn_manager->Begin(nullptr, context->log_mgr_);
    *txn_id = context->txn_->GetTransactionId();
    context->txn_->SetTxnMode(false);
  }
}

void *client_handler(void *sock_fd) {
  int fd = *((int *)sock_fd);
  pthread_mutex_unlock(sockfd_mutex);

  int i_recvBytes;
  // 接收客户端发送的请求
  char data_recv[BUFFER_LENGTH];
  // 需要返回给客户端的结果
  char *data_send = new char[BUFFER_LENGTH];
  std::vector<char> data_send_vec;
  // 需要返回给客户端的结果的长度
  int offset = 0;
  // 记录客户端当前正在执行的事务ID
  txn_id_t txn_id = INVALID_TXN_ID;

  std::string output = "establish client connection, sockfd: " + std::to_string(fd) + "\n";
  std::cout << output;

  while (true) {
    std::cout << "Waiting for request..." << std::endl;
    memset(data_recv, 0, BUFFER_LENGTH);

    i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

    if (i_recvBytes == 0) {
      std::cout << "Maybe the client has closed" << std::endl;
      break;
    }
    if (i_recvBytes == -1) {
      std::cout << "Client read error!" << std::endl;
      break;
    }

    printf("i_recvBytes: %d \n ", i_recvBytes);

    if (strcmp(data_recv, "exit") == 0) {
      std::cout << "Client exit." << std::endl;
      break;
    }
    if (strcmp(data_recv, "crash") == 0) {
      std::cout << "Server crash" << std::endl;
      exit(1);
    }

    std::cout << "Read from client " << fd << ": " << data_recv << std::endl;

    memset(data_send, '\0', BUFFER_LENGTH);
    offset = 0;

    // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
    Context *context = new Context(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
    SetTransaction(&txn_id, context);
    context->InitJson();

    // 用于判断是否已经调用了yy_delete_buffer来删除buf
    bool finish_analyze = false;
    pthread_mutex_lock(buffer_mutex);
    YY_BUFFER_STATE buf = yy_scan_string(data_recv);
    if (yyparse() == 0) {
      if (ast::parse_tree != nullptr) {
        try {
          // analyze and rewrite
          std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
          yy_delete_buffer(buf);
          finish_analyze = true;
          pthread_mutex_unlock(buffer_mutex);
          // 优化器
          // std::cout << "Optimizer Enable: " << planner->GetEnableOptimizer() << std::endl;
          // if (!optimizer->bypass(query, context))
          {
            auto temp1 = std::chrono::high_resolution_clock::now();
            std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
            // portal
            if (plan->tag != easydb::T_Empty) {
              std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
              portal->run(portalStmt, ql_manager.get(), &txn_id, context);
              portal->drop();
            } else {
              std::string str = "empty set\n";
              memcpy(context->data_send_, str.c_str(), str.length());
              *(context->offset_) = str.length();
            }
            context->SetJsonMsg("success");
            auto temp2 = std::chrono::high_resolution_clock::now();
            std::cout << "sql time usage:"
                      << (double)std::chrono::duration_cast<std::chrono::microseconds>(temp2 - temp1).count() / 1000000
                      << std::endl
                      << std::endl;
          }
        } catch (TransactionAbortException &e) {
          // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
          std::string str = "abort\n";
          memcpy(data_send, str.c_str(), str.length());
          data_send[str.length()] = '\0';
          offset = str.length();
          context->SetJsonMsg("abort");

          // 回滚事务
          txn_manager->Abort(context->txn_, log_manager.get());
          std::cout << e.GetInfo() << std::endl;

          if (sm_manager->IsEnableOutput()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << str;
            outfile.close();
          }
        } catch (EASYDBError &e) {
          // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
          std::cerr << e.what() << std::endl;

          memcpy(data_send, e.what(), e.get_msg_len());
          data_send[e.get_msg_len()] = '\n';
          data_send[e.get_msg_len() + 1] = '\0';
          offset = e.get_msg_len() + 1;
          context->SetJsonMsg(e.what());

          // 将报错信息写入output.txt
          if (sm_manager->IsEnableOutput()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "failure\n";
            outfile.close();
          }
        }
      }
    } else {
      context->SetJsonMsg("syntax error");
    }
    if (finish_analyze == false) {
      yy_delete_buffer(buf);
      pthread_mutex_unlock(buffer_mutex);
    }

    // 如果是单条语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
    if (context->txn_->GetTxnMode() == false) {
      // 如果已经 abort，则无需提交事务
      if (context->txn_->GetState() != TransactionState::ABORTED) {
        txn_manager->Commit(context->txn_, context->log_mgr_);
      }
    }

    // context->PrintJsonMsg();
    // context->PrintJson();
    // context->SerializeTo(data_send_vec);
    context->SerializeToWithLimit(data_send_vec, 100);
    // std::cout << data_send_vec.data() << std::endl;

    // 释放系统资源
    delete context;

    // future TODO: 格式化 sql_handler.result, 传给客户端
    // send result with fixed format, use protobuf in the future
    int send = 0;
    if (for_web) {
      send = write(fd, data_send_vec.data(), data_send_vec.size());
    } else {
      send = write(fd, data_send, offset + 1);
    }
    if (send == -1) {
      std::cerr << "Client write error!" << std::endl;
      break;
    }
  }

  // Clear
  delete[] data_send;  // release memory.
  // SetTransaction maybe allocate some txn
  txn_manager->ReleaseTxnOfThread(std::this_thread::get_id());
  std::cout << "Terminating current client_connection..." << std::endl;
  close(fd);           // close a file descriptor.
  pthread_exit(NULL);  // terminate calling thread!
}

void start_server() {
  // init mutex
  buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
  sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(buffer_mutex, nullptr);
  pthread_mutex_init(sockfd_mutex, nullptr);

  int sockfd_server;
  int fd_temp;
  struct sockaddr_in s_addr_in {};

  // 初始化连接
  sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
  assert(sockfd_server != -1);
  int val = 1;
  setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

  // before bind(), set the attr of structure sockaddr.
  memset(&s_addr_in, 0, sizeof(s_addr_in));
  s_addr_in.sin_family = AF_INET;
  s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
  s_addr_in.sin_port = htons(SOCK_PORT);
  fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
  if (fd_temp == -1) {
    std::cout << "Bind error!" << std::endl;
    exit(1);
  }

  fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
  if (fd_temp == -1) {
    std::cout << "Listen error!" << std::endl;
    exit(1);
  }

  while (!should_exit) {
    std::cout << "Waiting for new connection..." << std::endl;
    pthread_t thread_id;
    struct sockaddr_in s_addr_client {};
    int client_length = sizeof(s_addr_client);

    if (setjmp(jmpbuf)) {
      std::cout << "Break from Server Listen Loop\n";
      break;
    }

    // Block here. Until server accepts a new connection.
    pthread_mutex_lock(sockfd_mutex);
    sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
    if (sockfd == -1) {
      std::cout << "Accept error!" << std::endl;
      continue;  // ignore current socket ,continue while loop.
    }

    // 和客户端建立连接，并开启一个线程负责处理客户端请求
    if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0) {
      std::cout << "Create thread fail!" << std::endl;
      break;  // break while loop
    }
  }

  // Clear
  std::cout << " Try to close all client-connection.\n";
  int ret = shutdown(sockfd_server, SHUT_WR);  // shut down the all or part of a full-duplex connection.
  if (ret == -1) {
    printf("%s\n", strerror(errno));
  }
  //    assert(ret != -1);
  sm_manager->CloseDB();
  std::cout << " DB has been closed.\n";
  std::cout << "Server shuts down." << std::endl;
}

void print_help() { std::cout << "Usage: ./easydb_server -p <port> -d <database>"; }

int main(int argc, char **argv) {
  std::string db_name;
  int opt;
  while ((opt = getopt(argc, argv, "d:p:hw")) > 0) {
    switch (opt) {
      case 'd':
        db_name = optarg;
        break;
      case 'p':
        SOCK_PORT = std::stoi(std::string(optarg));
        break;
      case 'h':
        print_help();
        exit(0);
      case 'w':
        for_web = true;
        break;
      default:
        break;
    }
  }

  if (db_name.empty()) {
    print_help();
    exit(0);
  }

  try {
    std::cout << "\n"
                 "███████  █████  ███████ ██    ██ ██████  ██████\n"
                 "██      ██   ██ ██       ██  ██  ██   ██ ██   ██\n"
                 "█████   ███████ ███████   ████   ██   ██ ██████\n"
                 "██      ██   ██      ██    ██    ██   ██ ██   ██\n"
                 "███████ ██   ██ ███████    ██    ██████  ██████\n"
                 "\n"
                 "Welcome to EASYDB!\n"
                 "Type 'help;' for help.\n"
                 "\n";
    // Database name is passed by args

    disk_manager = std::make_unique<DiskManager>(db_name);
    buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
    ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
    sm_manager =
        std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
    lock_manager = std::make_unique<LockManager>();
    txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
    planner = std::make_unique<Planner>(sm_manager.get());
    optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
    ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get());
    log_manager = std::make_unique<LogManager>(disk_manager.get());
    recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get(),
                                                 txn_manager.get(), log_manager.get());

    portal = std::make_unique<Portal>(sm_manager.get());
    analyze = std::make_unique<Analyze>(sm_manager.get());

    signal(SIGINT, sigint_handler);
    if (!sm_manager->IsDir(db_name)) {
      // Database not found, create a new one
      sm_manager->CreateDB(db_name);
    }
    // Open database
    sm_manager->OpenDB(db_name);

    // recovery database
    recovery->analyze();
    recovery->redo();
    recovery->undo();

    // 开启服务端，开始接受客户端连接
    start_server();
  } catch (EASYDBError &e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }
  return 0;
}
