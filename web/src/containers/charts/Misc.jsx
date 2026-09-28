// Copyright 2021 Red Hat, Inc
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



const buildResultLegendData = [
    {
        name: 'SUCCESS',
        // PF green-500
        symbol: { fill: '#3E8635' },
    },
    {
        name: 'FAILURE',
        // PF red-100
        symbol: { fill: '#C9190B' },
    },
    {
        name: 'RETRY_LIMIT',
        // PF red-300
        symbol: { fill: '#7D1007' },
    },
    {
        name: 'SKIPPED',
        // PF light-blue-200
        symbol: { fill: '#7CDBF3' },
    },
    {
        name: 'ABORTED',
        // PF gold-200
        symbol: { fill: '#F6D173' },
    },
    {
        name: 'MERGE_CONFLICT',
        // PF orange-200
        symbol: { fill: '#EF9234' },
    },
    {
        name: 'MERGE_ERROR',
        // PF orange-200
        symbol: { fill: '#EF9234' },
    },
    {
        name: 'MERGE_FAILURE',
        // PF orange-200
        symbol: { fill: '#EF9234' },
    },
    {
        name: 'NODE_FAILURE',
        // PF orange-300
        symbol: { fill: '#EC7A08' },
    },
    {
        name: 'TIMED_OUT',
        // PF orange-400
        symbol: { fill: '#C46100' },
    },
    {
        name: 'POST_FAILURE',
        // PF orange-500
        symbol: { fill: '#8F4700' },
    },
    {
        name: 'CONFIG_ERROR',
        // PF orange-600
        symbol: { fill: '#773D00' },
    },
    {
        name: 'RETRY',
        // PF orange-100
        symbol: { fill: '#F4B678' },
    },]

const buildsBarStyleMap = buildResultLegendData.reduce(
    (final, x) => ({ ...final, [x.name]: x.symbol.fill }), {}
)

const buildsBarStyle = {
    data: {
        fill: ({ datum }) => buildsBarStyleMap[datum.result]
    }
}

export { buildResultLegendData, buildsBarStyleMap, buildsBarStyle }
