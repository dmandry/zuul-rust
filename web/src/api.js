// Copyright 2018 Red Hat, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

import Axios from 'axios'

let authToken = undefined

export function setAuthToken(token) {
  authToken = token
}

function getHomepageUrl() {
  //
  // Discover serving location from href.
  //
  // This is only needed for sub-directory serving. Serving the application
  // from 'scheme://domain/' may simply default to 'scheme://domain/'
  //
  // Note that this is not enough for sub-directory serving,
  // The static files location also needs to be adapted with the 'homepage'
  // settings of the package.json file.
  //
  // This homepage url is used for the Router and Link resolution logic
  //
  let url = new URL(window.location.href)

  if ('BASE_URL' in import.meta.env) {
    url.pathname = import.meta.env.BASE_URL
  } else {
    url.pathname = ''
  }
  if (!url.pathname.endsWith('/')) {
    url.pathname = url.pathname + '/'
  }
  return url.origin + url.pathname
}

function getZuulUrl() {
  // Return the zuul root api absolute url
  const ZUUL_API = import.meta.env.REACT_APP_ZUUL_API
  let apiUrl

  if (ZUUL_API) {
    // Api url set at build time, use it
    apiUrl = ZUUL_API
  } else {
    // Api url is relative to homepage path
    apiUrl = getHomepageUrl() + 'api/'
  }
  if (!apiUrl.endsWith('/')) {
    apiUrl = apiUrl + '/'
  }
  if (!apiUrl.endsWith('/api/')) {
    apiUrl = apiUrl + 'api/'
  }
  // console.log('Api url is ', apiUrl)
  return apiUrl
}
const apiUrl = getZuulUrl()


function getStreamUrl(apiPrefix) {
  const streamUrl = (apiUrl + apiPrefix)
    .replace(/(http)(s)?:\/\//, 'ws$2://') + 'console-stream'
  // console.log('Stream url is ', streamUrl)
  return streamUrl
}

function getAuthToken() {
  return authToken
}

function makeRequest(url, method, data) {
  if (method === undefined) {
    method = 'get'
  }

  // This performs a simple GET and tries to detect if CORS errors are
  // due to proxy authentication errors.
  const instance = Axios.create({
    baseURL: apiUrl
  })

  if (authToken) {
    instance.defaults.headers.common['Authorization'] = 'Bearer ' + authToken
  }

  const config = {method, url, data}

  // First try the request as normal
  let res = instance.request(config).catch(err => {
    if (err.response === undefined) {
      // This is either a Network, DNS, or CORS error, but we can't tell which.
      // If we're behind an authz proxy, it's possible our creds have timed out
      // and the CORS error is because we're getting a redirect.
      // Apache mod_auth_mellon (and possibly other authz proxies) will avoid
      // issuing a redirect if X-Requested-With is set to 'XMLHttpRequest' and
      // will instead issue a 403.  We can use this to detect that case.
      instance.defaults.headers.common['X-Requested-With'] = 'XMLHttpRequest'
      let res2 = instance.request(config).catch(err2 => {
        if (err2.response && err2.response.status === 403) {
          // We might be getting a redirect or something else,
          // so reload the page.
          console.log('Received 403 after unknown error; reloading')
          window.location.reload()
        }
        // If we're still getting an error, we don't know the cause,
        // it could be a transient network error, so we won't reload, we'll just
        // wait for it to clear.
        throw (err2)
      })
      return res2
    }
    throw (err)
  })
  return res
}

function getLogFile(url, authenticate, requestConfig = {}) {
  const apiOrigin = new URL(getZuulUrl()).origin
  const urlOrigin = new URL(url).origin
  let headers = {}

  // If we serve the log files from the same origin as the api, pass auth
  // headers to the log endpoint.
  // This is optional and can be enabled explicitly by setting the authenticate
  // parameter to true
  if (authenticate && authToken && urlOrigin === apiOrigin) {
    headers['Authorization'] = `Bearer ${authToken}`
  }
  const config = {headers: headers, ...requestConfig}
  return Axios.get(url, config)
}

// Direct APIs
function fetchInfo() {
  return makeRequest('info')
}

function fetchComponents() {
  return makeRequest('components')
}

function fetchTenantInfo(apiPrefix) {
  return makeRequest(apiPrefix + 'info')
}

function fetchOpenApi() {
  return Axios.get(getHomepageUrl() + 'openapi.yaml')
}

function fetchTenants() {
  return makeRequest(apiUrl + 'tenants')
}

function fetchTenantStatus(apiPrefix) {
  return makeRequest(apiPrefix + 'tenant-status')
}

function fetchConfigErrors(apiPrefix, queryString) {
  let path = 'config-errors'
  if (queryString) {
    path += '?' + queryString.slice(1)
  }
  return makeRequest(apiPrefix + path)
}

function fetchStatus(apiPrefix) {
  return makeRequest(apiPrefix + 'status')
}

function fetchSystemEvents(apiPrefix, queryString) {
  let path = 'system-events'
  if (queryString) {
    path += '?' + queryString.slice(1)
  }
  return makeRequest(apiPrefix + path)
}

function fetchChangeStatus(apiPrefix, changeId) {
  return makeRequest(apiPrefix + 'status/change/' + changeId)
}

function setTenantState(apiPrefix, discardTriggerEvents, pauseTriggerQueue, pauseResultQueue, reason) {
  return makeRequest(
    apiPrefix + 'state',
    'post',
    {
      trigger_queue_discarding: discardTriggerEvents,
      trigger_queue_paused: pauseTriggerQueue,
      result_queue_paused: pauseResultQueue,
      reason: reason,
    }
  )
}

function fetchFreezeJob(apiPrefix, pipelineName, projectName, branchName, jobName) {
  return makeRequest(apiPrefix +
                   'pipeline/' + pipelineName +
                   '/project/' + projectName +
                   '/branch/' + branchName +
                   '/freeze-job/' + jobName)
}

function fetchBuild(apiPrefix, buildId) {
  return makeRequest(apiPrefix + 'build/' + buildId)
}

function fetchBuilds(apiPrefix, queryString) {
  let path = 'builds'
  if (queryString) {
    path += '?' + queryString.slice(1)
  }
  return makeRequest(apiPrefix + path)
}

function fetchBuildTimes(apiPrefix, queryString) {
  let path = 'build-times'
  if (queryString) {
    path += '?' + queryString.slice(1)
  }
  return makeRequest(apiPrefix + path)
}

function fetchBuildset(apiPrefix, buildsetId) {
  return makeRequest(apiPrefix + 'buildset/' + buildsetId)
}

function fetchBuildsets(apiPrefix, queryString) {
  let path = 'buildsets'
  if (queryString) {
    path += '?' + queryString.slice(1)
  }
  return makeRequest(apiPrefix + path)
}

function fetchImages(apiPrefix) {
  return makeRequest(apiPrefix + 'images')
}

function buildImage(apiPrefix, imageName) {
  return makeRequest(
    apiPrefix + 'image/' + imageName + '/build',
    'post'
  )
}

function deleteImageBuildArtifact(apiPrefix, artifactId) {
  return makeRequest(
    apiPrefix + 'image-build-artifact/' + artifactId,
    'delete'
  )
}

function deleteImageUpload(apiPrefix, uploadId) {
  return makeRequest(
    apiPrefix + 'image-upload/' + uploadId,
    'delete'
  )
}

function validateImageUpload(apiPrefix, uploadId) {
  return makeRequest(
    apiPrefix + 'image-upload/' + uploadId + '/validate',
    'post'
  )
}

function retryImageUpload(apiPrefix, uploadId) {
  return makeRequest(
    apiPrefix + 'image-upload/' + uploadId + '/upload',
    'post'
  )
}

function fetchFlavors(apiPrefix) {
  return makeRequest(apiPrefix + 'flavors')
}

function fetchPipelines(apiPrefix) {
  return makeRequest(apiPrefix + 'pipelines')
}

function fetchProject(apiPrefix, projectName) {
  return makeRequest(apiPrefix + 'project/' + projectName)
}

function fetchProjects(apiPrefix) {
  return makeRequest(apiPrefix + 'projects')
}

function fetchProviders(apiPrefix) {
  return makeRequest(apiPrefix + 'providers')
}

function fetchJob(apiPrefix, jobName) {
  return makeRequest(apiPrefix + 'job/' + jobName)
}

function fetchJobGraph(apiPrefix, projectName, pipelineName, branchName) {
  return makeRequest(apiPrefix +
                   'pipeline/' + pipelineName +
                   '/project/' + projectName +
                   '/branch/' + branchName +
                   '/freeze-jobs')
}

function fetchJobs(apiPrefix) {
  return makeRequest(apiPrefix + 'jobs')
}

function fetchLabels(apiPrefix) {
  return makeRequest(apiPrefix + 'labels')
}

function fetchNodesetRequests(apiPrefix) {
  return makeRequest(apiPrefix + 'nodeset-requests')
}

function deleteNodesetRequest(apiPrefix, requestId) {
  return makeRequest(
    apiPrefix + '/nodeset-requests/' + requestId,
    'delete'
  )
}

function fetchNodes(apiPrefix) {
  return makeRequest(apiPrefix + 'nodes')
}

function setNodeState(apiPrefix, requestId, state) {
  return makeRequest(
    apiPrefix + '/nodes/' + requestId,
    'put',
    { state }
  )
}
function fetchSemaphores(apiPrefix) {
  return makeRequest(apiPrefix + 'semaphores')
}

function fetchAutoholds(apiPrefix) {
  return makeRequest(apiPrefix + 'autohold')
}

function fetchAutohold(apiPrefix, requestId) {
  return makeRequest(apiPrefix + 'autohold/' + requestId)
}

function fetchUserAuthorizations(apiPrefix) {
  return makeRequest(apiPrefix + 'authorizations')
}

function dequeue(apiPrefix, projectName, pipeline, change) {
  return makeRequest(
    apiPrefix + 'project/' + projectName + '/dequeue',
    'post',
    {
      pipeline: pipeline,
      change: change,
    }
  )
}

function dequeue_ref(apiPrefix, projectName, pipeline, ref) {
  return makeRequest(
    apiPrefix + 'project/' + projectName + '/dequeue',
    'post',
    {
      pipeline: pipeline,
      ref: ref,
    }
  )
}

function enqueue(apiPrefix, projectName, pipeline, change) {
  return makeRequest(
    apiPrefix + 'project/' + projectName + '/enqueue',
    'post',
    {
      pipeline: pipeline,
      change: change,
    }
  )
}

function enqueue_ref(apiPrefix, projectName, pipeline, ref, oldrev, newrev, params) {
  return makeRequest(
    apiPrefix + 'project/' + projectName + '/enqueue',
    'post',
    {
      pipeline: pipeline,
      ref: ref,
      oldrev: oldrev,
      newrev: newrev,
      parameters: params,
    }
  )
}

function autohold(apiPrefix, projectName, job, change, ref,
                  reason, count, node_hold_expiration) {
  return makeRequest(
    apiPrefix + 'project/' + projectName + '/autohold',
    'post',
    {
      change: change,
      job: job,
      ref: ref,
      reason: reason,
      count: count,
      node_hold_expiration: node_hold_expiration,
    }
  )
}

function autohold_delete(apiPrefix, requestId) {
  return makeRequest(
    apiPrefix + '/autohold/' + requestId,
    'delete'
  )
}

function promote(apiPrefix, pipeline, changes) {
  return makeRequest(
    apiPrefix + '/promote',
    'post',
    {
      pipeline: pipeline,
      changes: changes,
    }
  )
}

function freezePipeline(apiPrefix, pipeline, project, ref) {
  return makeRequest(
    apiPrefix + 'project/' + project + '/freeze-pipeline',
    'post',
    {
      pipeline: pipeline,
      ref: ref,
    }
  )
}

export {
  apiUrl,
  autohold,
  autohold_delete,
  buildImage,
  deleteImageBuildArtifact,
  deleteImageUpload,
  deleteNodesetRequest,
  dequeue,
  dequeue_ref,
  enqueue,
  enqueue_ref,
  fetchAutohold,
  fetchAutoholds,
  fetchBuild,
  fetchBuildTimes,
  fetchBuilds,
  fetchBuildset,
  fetchBuildsets,
  fetchChangeStatus,
  fetchComponents,
  fetchConfigErrors,
  fetchFlavors,
  fetchFreezeJob,
  fetchImages,
  fetchInfo,
  fetchJob,
  fetchJobGraph,
  fetchJobs,
  fetchLabels,
  fetchNodesetRequests,
  fetchNodes,
  fetchOpenApi,
  fetchPipelines,
  fetchProject,
  fetchProjects,
  fetchProviders,
  fetchSemaphores,
  fetchStatus,
  fetchSystemEvents,
  fetchTenantInfo,
  fetchTenantStatus,
  fetchTenants,
  fetchUserAuthorizations,
  getAuthToken,
  getHomepageUrl,
  getLogFile,
  getStreamUrl,
  promote,
  retryImageUpload,
  setNodeState,
  setTenantState,
  validateImageUpload,
  freezePipeline,
}
