/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include <hdfs/hdfs.h>
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsWriteFile.h"
#include "velox/core/Context.h"
#include <unordered_map>
#include <mutex>

namespace facebook::velox::filesystems {
folly::once_flag hdfsInitiationFlag;
std::string_view HdfsFileSystem::kScheme("hdfs://");
std::mutex mtx;

class HdfsFileSystem::Impl {
 public:
 explicit Impl(const Config* config) {
    auto endpointInfo = getServiceEndpoint(config);
    auto builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, endpointInfo.host.c_str());
    hdfsBuilderSetNameNodePort(builder, endpointInfo.port);
    hdfsClient_ = hdfsBuilderConnect(builder);
    VELOX_CHECK_NOT_NULL(
        hdfsClient_,
        "Unable to connect to HDFS, got error: {}.",
        hdfsGetLastError())
  }

  explicit Impl(const Config* config, const HdfsServiceEndpoint& endpoint) {
    auto builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, endpoint.host.c_str());
    hdfsBuilderSetNameNodePort(builder, endpoint.port);
    hdfsClient_ = hdfsBuilderConnect(builder);
    VELOX_CHECK_NOT_NULL(
        hdfsClient_,
        "Unable to connect to HDFS, got error: {}.",
        hdfsGetLastError())
  }

  ~Impl() {
    LOG(INFO) << "Disconnecting HDFS file system";
    int disconnectResult = hdfsDisconnect(hdfsClient_);
    if (disconnectResult != 0) {
      LOG(WARNING) << "hdfs disconnect failure in HdfsReadFile close: "
                   << errno;
    }
  }

  hdfsFS hdfsClient() {
    return hdfsClient_;
  }

 private:
  hdfsFS hdfsClient_;
};

HdfsFileSystem::HdfsFileSystem(const std::shared_ptr<const Config>& config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

HdfsFileSystem::HdfsFileSystem(const std::shared_ptr<const Config>& config, const HdfsServiceEndpoint& endpoint)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get(), endpoint);
}

std::string HdfsFileSystem::name() const {
  return "HDFS";
}

std::unique_ptr<ReadFile> HdfsFileSystem::openFileForRead(
    std::string_view path) {
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
  }
  if (auto index = path.find('/')) {
    path.remove_prefix(index);
  }

  return std::make_unique<HdfsReadFile>(impl_->hdfsClient(), path);
}

std::unique_ptr<WriteFile> HdfsFileSystem::openFileForWrite(
    std::string_view path) {
  return std::make_unique<HdfsWriteFile>(impl_->hdfsClient(), path);
}

bool HdfsFileSystem::isHdfsFile(const std::string_view filePath) {
  return filePath.find(kScheme) == 0;
}

/**
 * Get hdfs endpoint from config. This is applicable to the case that only one
 * hdfs endpoint will be used.
*/
HdfsServiceEndpoint HdfsFileSystem::getServiceEndpoint(const Config* config) {
    auto hdfsHost = config->get("hive.hdfs.host");
    VELOX_CHECK(
        hdfsHost.hasValue(),
        "hdfsHost is empty, configuration missing for hdfs host");
    auto hdfsPort = config->get("hive.hdfs.port");
    VELOX_CHECK(
        hdfsPort.hasValue(),
        "hdfsPort is empty, configuration missing for hdfs port");
    HdfsServiceEndpoint endpoint{*hdfsHost, atoi(hdfsPort->data())};
    return endpoint;
  }

/**
 * Get hdfs endpoint from file path, instead of getting a fixed one from configuraion.
*/
HdfsServiceEndpoint HdfsFileSystem::getServiceEndpoint(const std::string_view filePath) {
     auto index1 = filePath.find(':', kScheme.size());
     std::string hdfsHost{filePath.data(), kScheme.size(), index1 - kScheme.size()};
     VELOX_CHECK(
        !hdfsHost.empty(),
        "hdfsHost is empty, expect hdfs endpoint host is contained in file path");
     auto index2 = filePath.find('/', index1);
     std::string hdfsPort{filePath.data(), index1 + 1, index2 - index1 - 1};
     VELOX_CHECK(
        !hdfsPort.empty(),
        "hdfsPort is empty, expect hdfs endpoint port is contained in file path");
     HdfsServiceEndpoint endpoint{hdfsHost, atoi(hdfsPort.data())};
     return endpoint;
}

static std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const Config>, std::string_view)>
    filesystemGenerator = [](std::shared_ptr<const Config> properties, std::string_view filePath) {
      std::unordered_map<std::string, std::shared_ptr<FileSystem>> filesystems;
      static std::shared_ptr<FileSystem> filesystem;
      auto endpoint = HdfsFileSystem::getServiceEndpoint(filePath);
      std::string hdfsHostPort = endpoint.host + std::to_string(endpoint.port);
      if (filesystems.find(hdfsHostPort) != filesystems.end()) {
        return filesystems[hdfsHostPort];
      }
      mtx.lock();
      if (filesystems.find(hdfsHostPort) != filesystems.end()) {
        return filesystems[hdfsHostPort];
      }
      filesystem = std::make_shared<HdfsFileSystem>(properties, endpoint);
      // folly::call_once(hdfsInitiationFlag, [&properties]() {
      //   filesystem = std::make_shared<HdfsFileSystem>(properties, std::string_view host, std::string_view port);
      // });
      filesystems[hdfsHostPort] = filesystem;
      mtx.unlock();
      return filesystem;
    };

void HdfsFileSystem::remove(std::string_view path) {
  VELOX_UNSUPPORTED("Does not support removing files from hdfs");
}

void registerHdfsFileSystem() {
  registerFileSystem(HdfsFileSystem::isHdfsFile, filesystemGenerator);
}
} // namespace facebook::velox::filesystems
