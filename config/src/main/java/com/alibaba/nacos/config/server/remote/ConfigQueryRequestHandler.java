/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.remote;

import com.alibaba.nacos.api.config.remote.request.ConfigQueryRequest;
import com.alibaba.nacos.api.config.remote.response.ConfigQueryResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.common.ActionTypes;
import com.alibaba.nacos.config.server.auth.ConfigResourceParser;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.CacheItem;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.DiskUtil;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.remote.control.TpsControl;
import org.apache.commons.io.FileUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static com.alibaba.nacos.api.common.Constants.ENCODE;
import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;
import static com.alibaba.nacos.config.server.utils.RequestUtil.CLIENT_APPNAME_HEADER;

/**
 * ConfigQueryRequestHandler.
 *
 * @author liuzunfei
 * @version $Id: ConfigQueryRequestHandler.java, v 0.1 2020年07月14日 9:54 AM liuzunfei Exp $
 */
@Component
public class ConfigQueryRequestHandler extends RequestHandler<ConfigQueryRequest, ConfigQueryResponse> {
    
    private static final int TRY_GET_LOCK_TIMES = 9;
    
    private final PersistService persistService;
    
    public ConfigQueryRequestHandler(PersistService persistService) {
        this.persistService = persistService;
    }
    
    @Override
    @TpsControl(pointName = "ConfigQuery", parsers = {ConfigQueryGroupKeyParser.class, ConfigQueryGroupParser.class})
    @Secured(action = ActionTypes.READ, parser = ConfigResourceParser.class)
    public ConfigQueryResponse handle(ConfigQueryRequest request, RequestMeta meta) throws NacosException {
        
        try {
            return getContext(request, meta, request.isNotify());
        } catch (Exception e) {
            return ConfigQueryResponse.buildFailResponse(ResponseCode.FAIL.getCode(), e.getMessage());
        }
        
    }
    
    private ConfigQueryResponse getContext(ConfigQueryRequest configQueryRequest, RequestMeta meta, boolean notify)
            throws UnsupportedEncodingException {
        // 获取参数信息
        String dataId = configQueryRequest.getDataId();
        String group = configQueryRequest.getGroup();
        String tenant = configQueryRequest.getTenant();
        String clientIp = meta.getClientIp();
        String tag = configQueryRequest.getTag();
        // 创建response
        ConfigQueryResponse response = new ConfigQueryResponse();
        
        // 创建key  dataId+group+tenant
        final String groupKey = GroupKey2
                .getKey(configQueryRequest.getDataId(), configQueryRequest.getGroup(), configQueryRequest.getTenant());
        
        // 获取header中的 VIPSERVER_TAG
        String autoTag = configQueryRequest.getHeader(com.alibaba.nacos.api.common.Constants.VIPSERVER_TAG);
        
        // 获取app Name
        String requestIpApp = meta.getLabels().get(CLIENT_APPNAME_HEADER);
        
        // 通过group key 获取锁
        int lockResult = tryConfigReadLock(groupKey);
        
        boolean isBeta = false;
        boolean isSli = false;
        if (lockResult > 0) {
            //FileInputStream fis = null;
            //如果获取到锁
            try {
                String md5 = Constants.NULL;
                long lastModified = 0L;
                //获取缓存的cacheItem
                CacheItem cacheItem = ConfigCacheService.getContentCache(groupKey);
                if (cacheItem != null) {
                    //判断是否是测试版本配置
                    if (cacheItem.isBeta()) {
                        // 判断当前请求的clientIp是否是测试ip
                        if (cacheItem.getIps4Beta().contains(clientIp)) {
                            // 如果是返回测试版本配置，查询config_info_beta表
                            isBeta = true;
                        }
                    }
                    // 设置配置类型
                    String configType = cacheItem.getType();
                    response.setContentType((null != configType) ? configType : "text");
                }
                File file = null;
                ConfigInfoBase configInfoBase = null;
                PrintWriter out = null;
                if (isBeta) {
                    //获取测试版本配置信息
                    // 设置MD5值为cacheItem中的MD5测试版本的值
                    md5 = cacheItem.getMd54Beta();
                    //设置最后修改时间
                    lastModified = cacheItem.getLastModifiedTs4Beta();
                    // 如果是独立模式运行，并且使用嵌入式存储derby,那么就直接从derby库中读取
                    if (PropertyUtil.isDirectRead()) {
                        configInfoBase = persistService.findConfigInfo4Beta(dataId, group, tenant);
                    } else {
                        // 如果使用的是mysql，就从文件中获取数据
                        file = DiskUtil.targetBetaFile(dataId, group, tenant);
                    }
                    response.setBeta(true);
                } else {
                    // 获取非测试版本信息
                    if (StringUtils.isBlank(tag)) {
                        if (isUseTag(cacheItem, autoTag)) {
                            //如果缓存的数据中包含这个tag 就获取config_info_tag的数据
                            if (cacheItem != null) {
                                // 设置MD5值和最后修改时间
                                if (cacheItem.tagMd5 != null) {
                                    md5 = cacheItem.tagMd5.get(autoTag);
                                }
                                if (cacheItem.tagLastModifiedTs != null) {
                                    lastModified = cacheItem.tagLastModifiedTs.get(autoTag);
                                }
                            }
                            // 判断是否能读库
                            if (PropertyUtil.isDirectRead()) {
                                configInfoBase = persistService.findConfigInfo4Tag(dataId, group, tenant, autoTag);
                            } else {
                                file = DiskUtil.targetTagFile(dataId, group, tenant, autoTag);
                            }
                            response.setTag(URLEncoder.encode(autoTag, Constants.ENCODE));
                            
                        } else {
                            //如果 tag 为空就获取config_info数据
                            md5 = cacheItem.getMd5();
                            lastModified = cacheItem.getLastModifiedTs();
                            if (PropertyUtil.isDirectRead()) {
                                configInfoBase = persistService.findConfigInfo(dataId, group, tenant);
                            } else {
                                file = DiskUtil.targetFile(dataId, group, tenant);
                            }
                            // 配置不存在 打印日志，给客户端返回300
                            if (configInfoBase == null && fileNotExist(file)) {
                                // FIXME CacheItem
                                // No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
                                ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1,
                                        ConfigTraceService.PULL_EVENT_NOTFOUND, -1, clientIp, false);
                                
                                // pullLog.info("[client-get] clientIp={}, {},
                                // no data",
                                // new Object[]{clientIp, groupKey});
                                
                                response.setErrorInfo(ConfigQueryResponse.CONFIG_NOT_FOUND, "config data not exist");
                                return response;
                            }
                        }
                    } else {
                        //  如果tag不为空 获取config_info_tag表的数据
                        if (cacheItem != null) {
                            // 设置MD5和最后修改时间
                            if (cacheItem.tagMd5 != null) {
                                md5 = cacheItem.tagMd5.get(tag);
                            }
                            if (cacheItem.tagLastModifiedTs != null) {
                                Long lm = cacheItem.tagLastModifiedTs.get(tag);
                                if (lm != null) {
                                    lastModified = lm;
                                }
                            }
                        }
                        // 判断是否读库
                        if (PropertyUtil.isDirectRead()) {
                            configInfoBase = persistService.findConfigInfo4Tag(dataId, group, tenant, tag);
                        } else {
                            file = DiskUtil.targetTagFile(dataId, group, tenant, tag);
                        }
                        // 配置不存在 给客户端返回300 code
                        if (configInfoBase == null && fileNotExist(file)) {
                            // FIXME CacheItem
                            // No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
                            ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1,
                                    ConfigTraceService.PULL_EVENT_NOTFOUND, -1, clientIp, false);
                            
                            // pullLog.info("[client-get] clientIp={}, {},
                            // no data",
                            // new Object[]{clientIp, groupKey});
                            
                            response.setErrorInfo(ConfigQueryResponse.CONFIG_NOT_FOUND, "config data not exist");
                            return response;
                            
                        }
                    }
                }
                
                response.setMd5(md5);
                
                if (PropertyUtil.isDirectRead()) {
                    // 从derby库中读取到了数据 ，直接设置返回值
                    response.setLastModified(lastModified);
                    response.setContent(configInfoBase.getContent());
                    response.setResultCode(ResponseCode.SUCCESS.getCode());
                    
                } else {
                    //read from file
                    // 需要从文件中读取数据
                    String content = null;
                    try {
                        // 读取文件中的数据设置返回值
                        content = readFileContent(file);
                        response.setContent(content);
                        response.setLastModified(lastModified);
                        response.setResultCode(ResponseCode.SUCCESS.getCode());
                    } catch (IOException e) {
                        response.setErrorInfo(ResponseCode.FAIL.getCode(), e.getMessage());
                        return response;
                    }
                    
                }
                
                LogUtil.PULL_CHECK_LOG.warn("{}|{}|{}|{}", groupKey, clientIp, md5, TimeUtils.getCurrentTimeStr());
                
                final long delayed = System.currentTimeMillis() - lastModified;
                
                // 记录拉取事件
                // TODO distinguish pull-get && push-get
                /*
                 Otherwise, delayed cannot be used as the basis of push delay directly,
                 because the delayed value of active get requests is very large.
                 */
                ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, lastModified,
                        ConfigTraceService.PULL_EVENT_OK, notify ? delayed : -1, clientIp, notify);
                
            } finally {
                // 释放锁资源
                releaseConfigReadLock(groupKey);
            }
        } else if (lockResult == 0) {
            // 获取锁失败，这里打印log 并且给客户端返回code 300，客户端会删除本地的快照
            // FIXME CacheItem No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
            ConfigTraceService
                    .logPullEvent(dataId, group, tenant, requestIpApp, -1, ConfigTraceService.PULL_EVENT_NOTFOUND, -1,
                            clientIp, notify);
            response.setErrorInfo(ConfigQueryResponse.CONFIG_NOT_FOUND, "config data not exist");
            
        } else {
            PULL_LOG.info("[client-get] clientIp={}, {}, get data during dump", clientIp, groupKey);
            response.setErrorInfo(ConfigQueryResponse.CONFIG_QUERY_CONFLICT,
                    "requested file is being modified, please try later.");
        }
        return response;
    }
    
    /**
     * read content.
     *
     * @param file file to read.
     * @return content.
     */
    public static String readFileContent(File file) throws IOException {
        return FileUtils.readFileToString(file, ENCODE);
        
    }
    
    private static void releaseConfigReadLock(String groupKey) {
        ConfigCacheService.releaseReadLock(groupKey);
    }
    
    private static boolean fileNotExist(File file) {
        return file == null || !file.exists();
    }
    
    private static int tryConfigReadLock(String groupKey) {
        
        // Lock failed by default.
        int lockResult = -1;
        
        // Try to get lock times, max value: 10;
        for (int i = TRY_GET_LOCK_TIMES; i >= 0; --i) {
            lockResult = ConfigCacheService.tryReadLock(groupKey);
            
            // The data is non-existent.
            if (0 == lockResult) {
                break;
            }
            
            // Success
            if (lockResult > 0) {
                break;
            }
            
            // Retry.
            if (i > 0) {
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    LogUtil.PULL_CHECK_LOG.error("An Exception occurred while thread sleep", e);
                }
            }
        }
        
        return lockResult;
    }
    
    private static boolean isUseTag(CacheItem cacheItem, String tag) {
        if (cacheItem != null && cacheItem.tagMd5 != null && cacheItem.tagMd5.size() > 0) {
            return StringUtils.isNotBlank(tag) && cacheItem.tagMd5.containsKey(tag);
        }
        return false;
    }
    
}
