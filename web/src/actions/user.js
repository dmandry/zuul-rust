// Copyright 2020 Red Hat, Inc
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


import * as API from '../api'
import { USER_ACL_FAIL, USER_ACL_REQUEST, USER_ACL_SUCCESS } from './auth'

export const USER_LOGGED_IN = 'USER_LOGGED_IN'
export const USER_LOGGED_OUT = 'USER_LOGGED_OUT'

// Access tokens are not necessary JWTs (Google OAUTH uses a custom format)
// check the access token, if it isn't a JWT, use the ID token

export function getToken(user) {
  try {
    JSON.parse(atob(user.access_token.split('.')[1]))
    return user.access_token
  } catch {
    return user.id_token
  }
}

export const fetchUserACLRequest = (tenant) => ({
  type: USER_ACL_REQUEST,
  tenant: tenant,
})

export const userLoggedIn = (user, redirect) => (dispatch) => {
  const token = getToken(user)
  API.setAuthToken(token)
  dispatch({
    type: USER_LOGGED_IN,
    user: user,
    token: token,
    redirect: redirect,
  })
}

export const userLoggedOut = () => (dispatch) => {
  dispatch({
    type: USER_LOGGED_OUT,
  })
}

const fetchUserACLSuccess = (json) => ({
  type: USER_ACL_SUCCESS,
  isAdmin: json.zuul.admin,
  permissions: json.zuul.permissions,
  scope: json.zuul.scope,
})

const fetchUserACLFail = error => ({
  type: USER_ACL_FAIL,
  error
})

export const fetchUserACL = (tenant) => (dispatch) => {
  dispatch(fetchUserACLRequest(tenant? tenant.name : null))
  // tenant.name will be null if we're at the root
  const apiPrefix = tenant && tenant.name ? tenant.apiPrefix : ''
  return API.fetchUserAuthorizations(apiPrefix)
    .then(response => dispatch(fetchUserACLSuccess(response.data)))
    .catch(error => {
      dispatch(fetchUserACLFail(error))
    })
}
