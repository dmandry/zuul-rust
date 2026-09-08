# Copyright (C) 2023 Acme Gating, LLC
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.

from collections.abc import MutableMapping

from ansible.plugins.action import ActionBase
from jinja2.runtime import Undefined


# This method is from Ansible 2.18, GPLv3 Licensed.  It is not
# included in later versions of Ansible, but for safety in variable
# freezing, we still want to perform this check.
def recursive_check_defined(item):
    if isinstance(item, MutableMapping):
        for key in item:
            recursive_check_defined(item[key])
    elif isinstance(item, list):
        for i in item:
            recursive_check_defined(i)
    else:
        if isinstance(item, Undefined):
            raise Exception("{0} is undefined".format(item))


class ActionModule(ActionBase):
    def run(self, tmp=None, task_vars=None):
        """This module accepts one parameter:

        _zuul_freeze_vars is a list of variable names to template.

        It stores the templated variables in a cacheable fact named
        _zuul_frozen.

        If any variable is undefined or recursively references any
        undefined variables, it is omitted from the result.

        """
        if task_vars is None:
            task_vars = dict()
        results = super(ActionModule, self).run(tmp, task_vars)
        del tmp  # tmp no longer has any effect

        varlist = self._task.args.get('_zuul_freeze_vars')
        ret = {}
        for var in varlist:
            try:
                # Template the variable (convert_bare means treat a
                # bare variable name as {{ var }}.
                value = self._templar.template(var, convert_bare=True)
                recursive_check_defined(value)
                ret[var] = value
            except Exception:
                pass

        results['ansible_facts'] = {'_zuul_frozen': ret}
        results['_ansible_facts_cacheable'] = True

        return results
