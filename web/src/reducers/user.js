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


import {
  USER_LOGGED_IN,
  USER_LOGGED_OUT,
} from '../actions/user'
import {
  USER_ACL_REQUEST,
  USER_ACL_SUCCESS,
  USER_ACL_FAIL,
} from '../actions/auth'

export default (state = {
  isFetching: false,
  data: null,
  permissions: {},
  scope: [],
  isAdmin: false,
  // undefined tenant means we haven't loaded anything yet; null means
  // outside of tenant context.
  tenant: undefined,
  redirect: null,
}, action) => {
  switch (action.type) {
    case USER_LOGGED_IN: {
      return {
        isFetching: false,
        data: action.user,
        token: action.token,
        redirect: action.redirect,
        permissions: {},
        scope: [],
        isAdmin: false,
        tenant: undefined,
      }
    }
    case USER_LOGGED_OUT:
      return {
        isFetching: false,
        data: null,
        token: null,
        redirect: null,
        permissions: {},
        scope: [],
        isAdmin: false,
        tenant: undefined,
      }
    case USER_ACL_REQUEST:
      return {
        ...state,
        tenant: action.tenant,
        isFetching: true
      }
    case USER_ACL_FAIL:
      return {
        ...state,
        isFetching: false,
        permissions: {},
        scope: [],
        isAdmin: false
      }
    case USER_ACL_SUCCESS:
      return {
        ...state,
        isFetching: false,
        permissions: action.permissions,
        scope: action.scope,
        isAdmin: action.isAdmin
      }
    default:
      return state
  }
}
