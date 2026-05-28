# Copyright 2024 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import inspect
import re
import yaml

from zuul.web import ZuulWeb, ZuulWebAPI


PARAM_RE = re.compile(':param (.*?) (.*?): (.*)')

spec = {
    'info': {
        'title': 'Zuul REST API',
        'version': 'v1',
        'description': 'Incomplete (work in progress) list of the endpoints.',
    },
    'openapi': '3.0.0',
    'tags': [
        {'name': 'tenant'},
    ],
    'paths': {},
}


def parse_docstring(doc):
    # This separates the overall method summary (to be used as the
    # endpoint summary in the openapi spec) from parameter types and
    # descriptions.
    summary = []
    params = {}
    pname = None
    ptype = None
    pbuf = []
    for line in doc.split('\n'):
        line = line.strip()
        if not line:
            continue
        m = PARAM_RE.match(line)
        if m:
            if pname:
                params[pname] = {
                    'type': ptype,
                    'desc': ' '.join(pbuf),
                }
            ptype, pname, pdesc = m.groups()
            if ptype == 'str':
                ptype = 'string'
            pbuf = [pdesc.strip()]
            continue
        if pname:
            pbuf.append(line.strip())
        else:
            summary.append(line.strip())
    if pname:
        params[pname] = {
            'type': ptype,
            'desc': ' '.join(pbuf),
        }
    return ' '.join(summary), params


def generate_spec():
    api = ZuulWebAPI
    route_map = ZuulWeb.generateRouteMap(api, None, True)
    # Iterate over each route
    for r in route_map.mapper.matchlist:
        # Some of our routes have globs in the variable names; remove
        # those so that "{project:.*}" becomes "{project}" to match
        # the function arguments.
        routepath = r.routepath.replace(':.*', '')
        # action is the ZuulWebAPI method name
        action = r.defaults['action']
        # handler is the ZuulWebAPI method itself
        handler = getattr(api, action, None)
        if handler is None:
            continue
        # This is our output; initialize it to an empty dict, or if
        # we're handling another instance of a previous route (for
        # example, different GET and POST methods), start with that.
        routespec = spec['paths'].setdefault(routepath, {})
        # Parse the docstring if available
        doc = handler.__doc__
        if doc:
            summary, doc_params = parse_docstring(doc)
        else:
            summary = ''
            doc_params = {}
        if r.conditions and r.conditions.get('method'):
            # If this route specifies methods, use that
            methods = r.conditions['method']
        else:
            # Otherwise assume this method only handles GET
            methods = ['GET']
        for method in methods:
            if method == 'OPTIONS':
                continue
            # The @openapi decorators set this attribute; initialize
            # the output dictionary for this route-method to that
            # value or the empty dict.
            default_methodspec = {
                'responses': {
                    '200': {
                        'description': 'Response not yet documented',
                    }
                }
            }
            methodspec = getattr(handler, '__openapi__', default_methodspec)
            routespec[method.lower()] = methodspec
            methodspec['summary'] = summary
            methodspec['operationId'] = action
            methodspec['tags'] = ['tenant']
            methodspec['parameters'] = []
            body_params = {}

            # All inputs should be in the method signature, so iterate
            # over that.
            seen_params = set()
            for handler_param in inspect.signature(
                    handler).parameters.values():
                paramspec = {}
                # See if this function argument appears in the route
                # path, if so, it's a "path" parameter, otherwise it's
                # a "query" param.
                in_path = '{' + handler_param.name + '}' in routepath
                required = False
                if handler_param.default is handler_param.empty:
                    # No default value; it's either in the path or we
                    # don't care about it.
                    if not in_path:
                        continue
                    required = True
                paramspec = {
                    'name': handler_param.name,
                    'in': 'path' if in_path else 'query',
                }
                # Merge in information from the docstring if available.
                doc_param = doc_params.get(handler_param.name)
                if doc_param:
                    paramspec['description'] = doc_param['desc']
                    paramspec['schema'] = {'type': doc_param['type']}
                else:
                    paramspec['schema'] = {'type': 'string'}
                if required:
                    paramspec['required'] = required

                seen_params.add(handler_param.name)
                methodspec['parameters'].append(paramspec)
            for doc_param_name, doc_param in doc_params.items():
                if doc_param_name not in seen_params:
                    body_params[doc_param_name] = {
                        'type': doc_param['type'],
                    }
            if body_params:
                methodspec['requestBody'] = {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': {
                                'type': 'object',
                                'properties': body_params,
                            }
                        }
                    }
                }
    return spec


def main():
    with open('web/public/openapi.yaml', 'w') as f:
        f.write(yaml.safe_dump(generate_spec(),
                               default_flow_style=False))


if __name__ == '__main__':
    main()
