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
  PREFERENCE_SET,
} from '../actions/preferences'
import { resolveDarkMode, setDarkMode } from '../Misc'

const stored_prefs = localStorage.getItem('preferences')
let default_prefs
if (stored_prefs === null) {
  default_prefs = {
    autoReload: true,
    theme: 'Auto'
  }
} else {
  default_prefs = JSON.parse(stored_prefs)
}

export default (state = {
  ...default_prefs
}, action) => {
  if (action.type === PREFERENCE_SET) {
    let newstate = { ...state, [action.key]: action.value }
    delete newstate.darkMode
    localStorage.setItem('preferences', JSON.stringify(newstate))
    let darkMode = resolveDarkMode(newstate.theme)
    setDarkMode(darkMode)
    return { ...newstate, darkMode: darkMode }
  }
  // TODO: this runs on every action (not just preferences actions);
  // we should find a better place for it, but for now, we minimize
  // the impact by attempting only to run it once on startup.
  if (state.darkMode === undefined) {
    let darkMode = resolveDarkMode(state.theme)
    setDarkMode(darkMode)
    return { ...state, darkMode: darkMode }
  }
  return state
}
