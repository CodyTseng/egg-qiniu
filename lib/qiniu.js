'use strict';

const assert = require('assert');
const qiniu = require('qiniu');

const FORM_UPLOADER = Symbol('form_uploader');
const RESUME_UPLOADER = Symbol('resume_uploader');
const BUCKET_MANAGER = Symbol('bucket_manager');
const OPER_MANAGER = Symbol('oper_manager');
const CDN_MANAGER = Symbol('cdn_manager');

module.exports = app => {
    app.addSingleton('qiniu', createClient);
};

function createClient(config, app) {
    assert(config.accessKey && config.secretKey && config.zone && config.bucket,
        `[egg-qiniu] 'config.ak: ${config.accessKey}', 'config.sk: ${config.secretKey}', 
        'config.zone: ${config.zone}', 'config.bucket: ${config.bucket}' are required`);

    return new Qiniu(config, app)
}

function Qiniu(config, app) {
    this.app = app;
    this.accessKey = config.accessKey;
    this.secretKey = config.secretKey;
    this.zone = config.zone;
    this.bucket = config.bucket;

    this.useHttpsDomain = config.useHttpsDomain || false;
    this.useCdnDomain = config.useCdnDomain || false;

    this._mac = new qiniu.auth.digest.Mac(this.accessKey, this.secretKey);

    this.isLog = config.isLog;
}

function _now() {
    return Math.floor(Date.now() / 1000);
}

Object.assign(Qiniu.prototype, qiniu);

Qiniu.prototype._config = function() {
    const config = new qiniu.conf.Config();
    config.zone = qiniu.zone[this.zone];
    config.useHttpsDomain = this.useHttpsDomain;
    config.useCdnDomain = this.useCdnDomain;

    return config;
}

/**
 * 文件上传
 * 
 */
Qiniu.prototype.FormUploader = function() {
    if (!this[FORM_UPLOADER]) {
        const config = this._config();
        this[FORM_UPLOADER] = new qiniu.form_up.FormUploader(config);
    }

    return this[FORM_UPLOADER];
}

Qiniu.prototype.ResumeUploader = function() {
    if (!this[RESUME_UPLOADER]) {
        const config = this._config();
        this[RESUME_UPLOADER] = new qiniu.resume_uploader.ResumeUploader(config);
    }

    return this[RESUME_UPLOADER];
}

Qiniu.prototype._getPutPolicy = function() {
    const needCreate = !this._putPolicy || this._putPolicy.expires < _now();

    if (needCreate) {
        const options = {
            scope: this.bucket
            // expires: 7200,
        };

        this._putPolicy = new qiniu.rs.PutPolicy(options);
    }

    return this._putPolicy;
}

Qiniu.prototype._getUploadToken = function() {
    const putPolicy = this._getPutPolicy();
    return putPolicy.uploadToken(this._mac);
}

Qiniu.prototype._formUpload = function(key, local, type) {
    const uploadToken = this._getUploadToken();
    const formUploader = this.FormUploader();
    const putExtra = new qiniu.form_up.PutExtra();

    return new Promise((resolve, reject) => {
        formUploader[type](uploadToken, key, local, putExtra, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] FormUpload.${type} error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] FormUpload.${type} respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody
            });
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    });
}

Qiniu.prototype._resumeUpload = function(key, local, type) {
    const uploadToken = this._getUploadToken();
    const resumeUploader = this.ResumeUploader();
    const putExtra = new qiniu.resume_up.PutExtra();

    return new Promise((resolve, reject) => {
        resumeUploader[type](uploadToken, key, local, putExtra, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] ResumeUpload.${type} error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] ResumeUpload.${type} respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody,
            });
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    });
}

Qiniu.prototype.putFile = function(key, localFile, isResume = false) {
    if (isResume) return this._resumeUpload(key, localFile, 'putFile');

    return this._formUpload(key, localFile, 'putFile');
}

Qiniu.prototype.putStream = function(key, localStream, isResume = false) {
    if (isResume) return this._resumeUpload(key, localStream, 'putStream');

    return this._formUpload(key, localStream, 'putStream');
}

Qiniu.prototype.put = function(key, local) {
    return this._formUpload(key, local, 'put');
}

/**
 * 资源管理
 * 
 */

Qiniu.prototype.BucketManager = function() {
    if (!this[BUCKET_MANAGER]) {
        const config = this._config();

        this[BUCKET_MANAGER] = new qiniu.rs.BucketManager(this._mac, config);
    }

    return this[BUCKET_MANAGER];
}

Qiniu.prototype._bucketManager = function(type, ...params) {
    const bucketManager = this.BucketManager();

    return new Promise((resolve, reject) => {
        bucketManager[type](...params, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] BucketManager.${type} error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] BucketManager.${type} respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody,
            });
        });
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    });
}

Qiniu.prototype.stat = function(key) {
    return this._bucketManager('stat', this.bucket, key);
}

Qiniu.prototype.changeMime = function(key, newMime) {
    return this._bucketManager('changeMime', this.bucket, key, newMime);
}

Qiniu.prototype.changeType = function(key, newType) {
    return this._bucketManager('changeType', this.bucket, key, newType);
}

Qiniu.prototype.delete = function(key) {
    return this._bucketManager('delete', this.bucket, key);
}

Qiniu.prototype.deleteAfterDays = function(key, days) {
    return this._bucketManager('deleteAfterDays', this.bucket, key, days);
}

Qiniu.prototype.listPrefix = function(options) {
    return this._bucketManager('listPrefix', this.bucket, options);
}

Qiniu.prototype.fetch = function(resUrl, key) {
    return this._bucketManager('fetch', resUrl, this.bucket, key);
}

Qiniu.prototype.prefetch = function(key) {
    return this._bucketManager('prefetch', this.bucket, key);
}

Qiniu.prototype.move = function(srcKey, destBucket, destKey, options) {
    return this._bucketManager('move', this.bucket, srcKey, destBucket, destKey, options);
}

Qiniu.prototype.copy = function(srcKey, destBucket, destKey, options) {
    return this._bucketManager('copy', this.bucket, srcKey, destBucket, destKey, options);
}

Qiniu.prototype._wrapBucketManagerOperation = function(type, ...params) {
    return qiniu.rs[type + 'Op'](this.bucket, ...params);
}

Qiniu.prototype.batch = function(options) {
    assert(typeof operations != 'object' || operations.constructor != Array, '[egg-qiniu] operations should be an array');

    const bucketManager = this.BucketManager();

    const operationsWrapped = operations.map(operation => {
        const [type, ...params] = operation;
        return this._wrapBucketManagerOperation(type, ...params);
    })

    return new Promise((resolve, reject) => {
        bucketManager.batch(operationsWrapped, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] BucketManager.batch error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] BucketManager.batch respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            if (respInfo.statusCode !== 200 || respInfo.statusCode !== 298) {
                resolve({
                    code: respInfo.statusCode,
                    data: respBody,
                });
            }

            const data = respBody.forEach(item => {
                return {
                    code: item.code,
                    data: item.data,
                }
            })
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    })
}

/**
 * 持久化数据处理
 * 
 */

Qiniu.prototype.OperManager = function() {
    if (!this[OPER_MANAGER]) {
        const config = this._config();
        this[OPER_MANAGER] = new qiniu.fop.OperationManager(this._mac, config);
    }

    return this[OPER_MANAGER];
}

Qiniu.prototype._encodeFops = function(fops) {
    return fops.map(item => {
        const sepIndex = item.lastIndexOf('/');
        return item.substring(0, sepIndex + 1) + qiniu.util.urlsafeBase64Encode(item.substring(sepIndex + 1));
    });
}

Qiniu.prototype._operManager = function(type, ...params) {
    const operManager = this.OperManager();

    return new Promise((resolve, reject) => {
        operManager[type](...params, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] OperManager.${type} error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] OperManager.${type} respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody,
            });
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    });
}

Qiniu.prototype.pfop = function(key, fops, pipeline, options) {
    const fopsEncoded = this._encodeFops(fops);

    return this._operManager('pfop', this.bucket, key, fopsEncoded, pipeline, options);
}

Qiniu.prototype.prefop = function(presistentId) {
    return this._operManager('prefop', presistentId);
}

/**
 * CDN 相关
 * 
 */

Qiniu.prototype.CdnManager = function() {
    if (!this[CDN_MANAGER]) {
        this[CDN_MANAGER] = new qiniu.cdn.CdnManager(this._mac);
    }

    return this[CDN_MANAGER];
}

Qiniu.prototype._cdnManager = function() {
    const cdnManager = thia._getCdnManager();

    return new Promise((resolve, reject) => {
        cdnManager[type](...params, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] CdnManager.${type} error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] CdnManager.${type} respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody,
            });
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    })
}

Qiniu.prototype.refreshUrls = function(urlsToRefresh) {
    return this._cdnManager('refreshUrls', urlsToRefresh);
}

Qiniu.prototype.refreshDirs = function(dirsToRefresh) {
    return new Promise((resolve, reject) => {
        qiniu.cdn.refreshDirs(dirsToRefresh, (respErr, respBody, respInfo) => {
            if (respErr) {
                this.app.coreLogger.error(`[egg-qiniu] CdnManager.refreshDirs error: ${respErr}`)
                reject(respErr);
            }

            if (this.isLog) {
                this.app.coreLogger.info(`[egg-qiniu] CdnManager.refreshDirs respBody: ${JSON.stringify(respBody)} respInfo: ${JSON.stringify(respInfo)}`);
            }

            resolve({
                code: respInfo.statusCode,
                data: respBody,
            });
        })
    }).catch(error => {
        return {
            code: 400,
            data: {
                error,
            },
        }
    })
}

Qiniu.prototype.prefetchUrls = function(urlsToPrefetch) {
    return this._cdnManager('prefetchUrls', urlsToPrefetch);
}

Qiniu.prototype.getFluxData = function(startDate, endDate, granularity, domains) {
    return this._cdnManager('getFluxData', startDate, endDate, granularity, domains);
}

Qiniu.prototype.getBandwidthData = function(startDate, endDate, granularity, domains) {
    return this._cdnManager('getBandwidthData', startDate, endDate, granularity, domains);
}

Qiniu.prototype.getCdnLogList = function(domains, logDay) {
    return this._cdnManager('getCdnLogList', domains, logDay);
}