%%
%% Licensed to the Apache Software Foundation (ASF) under one
%% or more contributor license agreements. See the NOTICE file
%% distributed with this work for additional information
%% regarding copyright ownership. The ASF licenses this file
%% to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance
%% with the License. You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(test_recursive).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-include("recursive_types.hrl").

encode_decode_recursive_test() ->
  {ok, Transport} = thrift_memory_buffer:new(),
  {ok, Protocol0} = thrift_binary_protocol:new(Transport),
  TestData = #coRec{other = #coRec2{other = #coRec{}}},
  {Protocol1, ok} = thrift_protocol:write(Protocol0,
    {{struct, element(2, recursive_types:struct_info('coRec'))},
      TestData}),
  {_Protocol2, {ok, Result}} = thrift_protocol:read(Protocol1,
    {struct, element(2, recursive_types:struct_info('coRec'))},
    'coRec'),
  Result = TestData.

encode_decode_recursive_2_test() ->
  {ok, Transport} = thrift_memory_buffer:new(),
  {ok, Protocol0} = thrift_binary_protocol:new(Transport),
  TestData = #recTree{item = 42, children = [#recTree{}, #recTree{item = 31337, children = [#recTree{}]}]},
  {Protocol1, ok} = thrift_protocol:write(Protocol0,
    {{struct, element(2, recursive_types:struct_info('recTree'))},
      TestData}),
  {_Protocol2, {ok, Result}} = thrift_protocol:read(Protocol1,
    {struct, element(2, recursive_types:struct_info('recTree'))},
    'recTree'),
  Result = TestData.

-endif.
