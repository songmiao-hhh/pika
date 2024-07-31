// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/redis_sender.h"

#include <time.h>
#include <unistd.h>
#include <algorithm>

#include <glog/logging.h>

#include "slash/include/xdebug.h"

static time_t kCheckDiff = 1;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string password, int my_db_num):
  id_(id),
  cli_(NULL),
  rsignal_(&commands_mutex_),
  wsignal_(&commands_mutex_),
  ip_(ip),
  port_(port),
  password_(password),
  should_exit_(false),
  cnt_(0),
  elements_(0),
  my_db_num_(my_db_num) {

  last_write_time_ = ::time(NULL);
}

RedisSender::~RedisSender() {
  LOG(INFO) << "RedisSender thread " << id_ << " exit!!!";
}

void RedisSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1000);
    cli_->set_recv_timeout(10000);
    cli_->set_send_timeout(10000);
    slash::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      delete cli_;
      cli_ = NULL;
      sleep(3);
      continue;
    } else {
      // Connect success

      // Authentication
      if (!password_.empty()) {
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
          } else {
            LOG(FATAL) << "Connect to redis(" << ip_ << ":" << port_ << ") Invalid password";
            cli_->Close();
            delete cli_;
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
          LOG(WARNING) << "send auth failed: " << s.ToString();
          cli_->Close();
          delete cli_;
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              LOG(FATAL) << "Ping redis(" << ip_ << ":" << port_ << ") NOAUTH Authentication required";
              cli_->Close();
              delete cli_;
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            LOG(WARNING) << s.ToString();
            cli_->Close();
            delete cli_;
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  commands_mutex_.Lock();
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

void RedisSender::SendRedisCommand(const std::string &table_name, const std::string &command) {
  commands_mutex_.Lock();
  if (dbname_commands_queue_.size() < 100000) {
    dbname_commands_queue_.push(std::make_pair(table_name, command));
    rsignal_.Signal();
    commands_mutex_.Unlock();
    return;
  }

  while (dbname_commands_queue_.size() > 100000) {
    wsignal_.Wait();
  }
  dbname_commands_queue_.push(std::make_pair(table_name, command));
  rsignal_.Signal();
  commands_mutex_.Unlock();
}

void RedisSender::CheckDatabases() {
  ConnectRedis();
  pink::RedisCmdArgsType argv, resp;
  std::string cmd;
  argv.push_back("config");
  argv.push_back("get");
  argv.push_back("databases");
  pink::SerializeRedisCommand(argv, &cmd);
  slash::Status s = cli_->Send(&cmd);

  if (s.ok()) {
    s = cli_->Recv(&resp);
    if (resp[0] == "databases") {
      std::string db_num = resp[1];
      LOG(INFO) << "checkdatabases, target_db_num:" << db_num << ", my_db_num:" << my_db_num_;
      if(!std::all_of(db_num.begin(), db_num.end(), ::isdigit)) {
        LOG(FATAL) << "DB num: " << db_num << "is not digit";
      }
      int target_db_num = std::stoi(db_num);
      if(target_db_num < my_db_num_) {
        LOG(FATAL) << "Target DB num is smaller than mine, target DB num: " << target_db_num << ", my DB num: " << my_db_num_;
      }
    } else {
      LOG(FATAL) << "Check databases fail, first resp is NOT databases, it is " << resp[0];
      cli_->Close();
      delete cli_;
      cli_ = NULL;
      should_exit_ = true;
    }
  } else {
    LOG(FATAL) << "Send get databases command failed: " << s.ToString();
    cli_->Close();
    delete cli_;
    cli_ = NULL;
  }    
}

int RedisSender::SendCommand(std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      ConnectRedis();
    }
    last_write_time_ = now;
  }

  // Send command
  int idx = 0;
  do {
    slash::Status s = cli_->Send(&command);
    if (s.ok()) {
      return 0;
    }

    LOG(WARNING) << "RedisSender " << id_ << "fails to send redis command " << command << ", times: " << idx + 1 << ", error: " << s.ToString();

    cli_->Close();
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  } while(++idx < 3);

  return -1;
}

void RedisSender::SelectDB(std::string &db_name) {
  if(db_name.length() <= 2) {
    LOG(FATAL) << "DB name:" << db_name << " length is too small";
  }
  if(strcmp(db_name.substr(0, 2).data(), "db")) {
    LOG(FATAL) << "DB name:" << db_name << " is NOT start with \"db\"";
  }

  std::string db_idx = db_name.substr(2);
  if(!std::all_of(db_idx.begin(), db_idx.end(), ::isdigit)) {
    LOG(FATAL) << db_idx << "is not digit";
  }

  pink::RedisCmdArgsType argv;
  std::string cmd;
  argv.push_back("SELECT");
  argv.push_back(db_idx);
  pink::SerializeRedisCommand(argv, &cmd);
  slash::Status s = cli_->Send(&cmd);
  if (!s.ok()) {
    cli_->Close();
    log_info("%s", s.ToString().data());
    delete cli_;
    cli_ = NULL;
    ConnectRedis();
  }
}

void *RedisSender::ThreadMain() {
  LOG(INFO) << "Start redis sender " << id_ << " thread...";
  // sleep(15);
  int ret = 0;

  ConnectRedis();
  CheckDatabases();

  while (!should_exit_) {
    commands_mutex_.Lock();
    while (dbname_commands_queue_.size() == 0 && !should_exit_) {
      rsignal_.TimedWait(100);
      // rsignal_.Wait();
    }
    // if (commands_queue_.size() == 0 && should_exit_) {
    if (should_exit_) {
      commands_mutex_.Unlock();
      break;
    }

    if (dbname_commands_queue_.size() == 0) {
      commands_mutex_.Unlock();
      continue;
    }
    commands_mutex_.Unlock();

    // get redis command
    std::string command;
    std::string db_name;
    commands_mutex_.Lock();
    db_name = dbname_commands_queue_.front().first;
    command = dbname_commands_queue_.front().second;
    // printf("%d, command %s\n", id_, command.c_str());
    elements_++;
    dbname_commands_queue_.pop();
    wsignal_.Signal();
    commands_mutex_.Unlock();
    SelectDB(db_name);
    ret = SendCommand(command);
    if (ret == 0) {
      cnt_++;
    }

    if (cnt_ >= 200) {
      for(; cnt_ > 0; cnt_--) {
        cli_->Recv(NULL);
      }
    }
  }
  for(; cnt_ > 0; cnt_--) {
    cli_->Recv(NULL);
  }

  LOG(INFO) << "RedisSender thread " << id_ << " complete";
  delete cli_;
  cli_ = NULL;
  return NULL;
}

