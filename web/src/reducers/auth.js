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
  AUTH_CONFIG_REQUEST,
  AUTH_CONFIG_SUCCESS,
  AUTH_CONFIG_FAIL,
} from '../actions/auth'

// Load the defaults from local storage if it exists so that we
// construct the same AuthProvider we had before we navigated to the
// IDP redirect.
const stored_params = localStorage.getItem('zuul_auth_params')
let auth_params = {
  authority: '',
  client_id: '',
  scope: '',
  loadUserInfo: true,
}
if (stored_params !== null) {
  auth_params = JSON.parse(stored_params)
}

export default (state = {
  isFetching: false,
  info: null,
  auth_params: auth_params,
}, action) => {
  const json_params = JSON.stringify(action.auth_params)
  switch (action.type) {
    case AUTH_CONFIG_REQUEST:
      return {
        ...state,
        isFetching: true,
      }
    case AUTH_CONFIG_SUCCESS:
      // Make sure we only update the auth_params object if something actually
      // changes.  Otherwise, it will re-create the AuthProvider which
      // may cause errors with auth state if it happens concurrently with
      // a login.
      if (json_params === JSON.stringify(state.auth_params)) {
        return {
          ...state,
          isFetching: false,
          info: action.info,
        }
      } else {
        localStorage.setItem('zuul_auth_params', json_params)
        return {
          ...state,
          isFetching: false,
          info: action.info,
          auth_params: action.auth_params,
        }
      }
    case AUTH_CONFIG_FAIL:
      return {
        ...state,
        isFetching: false,
        info: null,
      }
    default:
      return state
  }
}
