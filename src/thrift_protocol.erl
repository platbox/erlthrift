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

-module(thrift_protocol).

-export([new/2,
         write/2,
         read/2,
         read/3,
         skip/2,
         validate/2,
         flush_transport/1,
         close_transport/1,
         typeid_to_atom/1
        ]).

-export([behaviour_info/1]).

-include("thrift_constants.hrl").
-include("thrift_protocol.hrl").

-record(protocol, {module, data}).

behaviour_info(callbacks) ->
    [
     {read, 2},
     {write, 2},
     {flush_transport, 1},
     {close_transport, 1}
    ];
behaviour_info(_Else) -> undefined.

new(Module, Data) when is_atom(Module) ->
    {ok, #protocol{module = Module,
                   data = Data}}.

-spec flush_transport(#protocol{}) -> {#protocol{}, ok}.
flush_transport(Proto = #protocol{module = Module,
                                  data = Data}) ->
    {NewData, Result} = Module:flush_transport(Data),
    {Proto#protocol{data = NewData}, Result}.

-spec close_transport(#protocol{}) -> ok.
close_transport(#protocol{module = Module,
                          data = Data}) ->
    Module:close_transport(Data).

typeid_to_atom(?tType_STOP) -> field_stop;
typeid_to_atom(?tType_VOID) -> void;
typeid_to_atom(?tType_BOOL) -> bool;
typeid_to_atom(?tType_BYTE) -> byte;
typeid_to_atom(?tType_DOUBLE) -> double;
typeid_to_atom(?tType_I16) -> i16;
typeid_to_atom(?tType_I32) -> i32;
typeid_to_atom(?tType_I64) -> i64;
typeid_to_atom(?tType_STRING) -> string;
typeid_to_atom(?tType_STRUCT) -> struct;
typeid_to_atom(?tType_MAP) -> map;
typeid_to_atom(?tType_SET) -> set;
typeid_to_atom(?tType_LIST) -> list.

term_to_typeid(void) -> ?tType_VOID;
term_to_typeid(bool) -> ?tType_BOOL;
term_to_typeid(byte) -> ?tType_BYTE;
term_to_typeid(double) -> ?tType_DOUBLE;
term_to_typeid(i16) -> ?tType_I16;
term_to_typeid(i32) -> ?tType_I32;
term_to_typeid(i64) -> ?tType_I64;
term_to_typeid(string) -> ?tType_STRING;
term_to_typeid({enum, _}) -> ?tType_I32;
term_to_typeid({struct, _}) -> ?tType_STRUCT;
term_to_typeid({map, _, _}) -> ?tType_MAP;
term_to_typeid({set, _}) -> ?tType_SET;
term_to_typeid({list, _}) -> ?tType_LIST.

%% Structure is like:
%%    [{Fid, Type}, ...]
-spec read(#protocol{}, {struct, _StructDef}, atom()) -> {#protocol{}, {ok, tuple()}}.
read(IProto0, {struct, StructDef}, Tag)
  when is_list(StructDef), is_atom(Tag) ->

    % If we want a tagged tuple, we need to offset all the tuple indices
    % by 1 to avoid overwriting the tag.
    Offset = if Tag =/= undefined -> 1; true -> 0 end,

    {IProto1, ok} = read_(IProto0, struct_begin),
    RTuple0 = erlang:make_tuple(length(StructDef) + Offset, undefined),
    RTuple1 = if Tag =/= undefined -> setelement(1, RTuple0, Tag);
                 true              -> RTuple0
              end,

    StructIndex = zip(1 + Offset, StructDef),
    read_struct_loop(IProto1, StructIndex, RTuple1).

zip(_, []) ->
    [];
zip(N, [{Fid, _Req, Type, Name, _Default} | Rest]) ->
    [{N, Fid, Type, Name} | zip(N + 1, Rest)];
zip(N, [{Fid, Type} | Rest]) ->
    [{N, Fid, Type, Type} | zip(N + 1, Rest)].

%% NOTE: Keep this in sync with thrift_protocol_behaviour:read
-spec read
        (#protocol{}, {struct, _Info}) ->    {#protocol{}, {ok, tuple()}      | {error, _Reason}};
        (#protocol{}, tprot_cont_tag()) ->   {#protocol{}, {ok, any()}        | {error, _Reason}};
        (#protocol{}, tprot_empty_tag()) ->  {#protocol{},  ok                | {error, _Reason}};
        (#protocol{}, tprot_header_tag()) -> {#protocol{}, tprot_header_val() | {error, _Reason}};
        (#protocol{}, tprot_data_tag()) ->   {#protocol{}, {ok, any()}        | {error, _Reason}}.

read(IProto, Type) ->
    case Result = read_(IProto, Type) of
        {IProto2, {ok, Data}} ->
            case validate({Type, Data}) of
                ok -> Result;
                Error -> {IProto2, Error}
            end;
        _ ->
            Result
    end.

read_(IProto, {struct, {Module, StructureName}}) when is_atom(Module),
                                                     is_atom(StructureName) ->
    read(IProto, Module:struct_info_ext(StructureName), StructureName);

read_(IProto, S={struct, Structure}) when is_list(Structure) ->
    read(IProto, S, undefined);

read_(IProto, {enum, {Module, EnumName}}) when is_atom(Module) ->
    read_(IProto, Module:enum_info(EnumName));

read_(IProto, {enum, Fields}) when is_list(Fields) ->
    {IProto2, {ok, IVal}} = read_(IProto, i32),
    {EnumVal, IVal} = lists:keyfind(IVal, 2, Fields),
    {IProto2, {ok, EnumVal}};

read_(IProto0, {list, Type}) ->
    {IProto1, #protocol_list_begin{etype = EType, size = Size}} =
        read_(IProto0, list_begin),
    {EType, EType} = {term_to_typeid(Type), EType},
    {List, IProto2} = lists:mapfoldl(fun(_, ProtoS0) ->
                                             {ProtoS1, {ok, Item}} = read_(ProtoS0, Type),
                                             {Item, ProtoS1}
                                     end,
                                     IProto1,
                                     lists:duplicate(Size, 0)),
    {IProto3, ok} = read_(IProto2, list_end),
    {IProto3, {ok, List}};

read_(IProto0, {map, KeyType, ValType}) ->
    {IProto1, #protocol_map_begin{size = Size, ktype = KType, vtype = VType}} =
        read_(IProto0, map_begin),
    {KType, KType} = {term_to_typeid(KeyType), KType},
    {VType, VType} = {term_to_typeid(ValType), VType},
    {Map, IProto2} = lists:foldl(fun(_, {M, ProtoS0}) ->
                                             {ProtoS1, {ok, Key}} = read_(ProtoS0, KeyType),
                                             {ProtoS2, {ok, Val}} = read_(ProtoS1, ValType),
                                             {maps:put(Key, Val, M), ProtoS2}
                                     end,
                                     {#{}, IProto1},
                                     lists:duplicate(Size, 0)),
    {IProto3, ok} = read_(IProto2, map_end),
    {IProto3, {ok, Map}};

read_(IProto0, {set, Type}) ->
    {IProto1, #protocol_set_begin{etype = EType, size = Size}} =
        read_(IProto0, set_begin),
    {EType, EType} = {term_to_typeid(Type), EType},
    {List, IProto2} = lists:mapfoldl(fun(_, ProtoS0) ->
                                             {ProtoS1, {ok, Item}} = read_(ProtoS0, Type),
                                             {Item, ProtoS1}
                                     end,
                                     IProto1,
                                     lists:duplicate(Size, 0)),
    {IProto3, ok} = read_(IProto2, set_end),
    {IProto3, {ok, ordsets:from_list(List)}};

read_(Protocol, ProtocolType) ->
    read_specific(Protocol, ProtocolType).

%% NOTE: Keep this in sync with thrift_protocol_behaviour:read
-spec read_specific
        (#protocol{}, tprot_empty_tag()) ->  {#protocol{},  ok                | {error, _Reason}};
        (#protocol{}, tprot_header_tag()) -> {#protocol{}, tprot_header_val() | {error, _Reason}};
        (#protocol{}, tprot_data_tag()) ->   {#protocol{}, {ok, any()}        | {error, _Reason}}.
read_specific(Proto = #protocol{module = Module,
                                data = ModuleData}, ProtocolType) ->
    {NewData, Result} = Module:read(ModuleData, ProtocolType),
    {Proto#protocol{data = NewData}, Result}.

read_struct_loop(IProto0, StructIndex, RTuple) ->
    {IProto1, #protocol_field_begin{type = FType, id = Fid}} = read_(IProto0, field_begin),
    case {FType, Fid} of
        {?tType_STOP, _} ->
            {IProto1, {ok, RTuple}};
        _Else ->
            case lists:keyfind(Fid, 2, StructIndex) of
                {N, Fid, Type, Name} ->
                    case term_to_typeid(Type) of
                        FType ->
                            {IProto2, {ok, Val}} = read_(IProto1, Type),
                            {IProto3, ok} = read_(IProto2, field_end),
                            NewRTuple = setelement(N, RTuple, Val),
                            read_struct_loop(IProto3, StructIndex, NewRTuple);
                        _Expected ->
                            error_logger:info_msg("Skipping field ~p with wrong type: ~p~n", [Name, typeid_to_atom(FType)]),
                            skip_field(FType, IProto1, StructIndex, RTuple)
                    end;
                false ->
                    error_logger:info_msg("Skipping unknown field (~p) with type: ~p~n", [Fid, typeid_to_atom(FType)]),
                    skip_field(FType, IProto1, StructIndex, RTuple)
            end
    end.

skip_field(FType, IProto0, StructIndex, RTuple) ->
    FTypeAtom = thrift_protocol:typeid_to_atom(FType),
    {IProto1, ok} = skip(IProto0, FTypeAtom),
    {IProto2, ok} = read_(IProto1, field_end),
    read_struct_loop(IProto2, StructIndex, RTuple).

-spec skip(#protocol{}, any()) -> {#protocol{}, ok}.

skip(Proto0, struct) ->
    {Proto1, ok} = read_(Proto0, struct_begin),
    {Proto2, ok} = skip_struct_loop(Proto1),
    {Proto3, ok} = read_(Proto2, struct_end),
    {Proto3, ok};

skip(Proto0, map) ->
    {Proto1, Map} = read_(Proto0, map_begin),
    {Proto2, ok} = skip_map_loop(Proto1, Map),
    {Proto3, ok} = read_(Proto2, map_end),
    {Proto3, ok};

skip(Proto0, set) ->
    {Proto1, Set} = read_(Proto0, set_begin),
    {Proto2, ok} = skip_set_loop(Proto1, Set),
    {Proto3, ok} = read_(Proto2, set_end),
    {Proto3, ok};

skip(Proto0, list) ->
    {Proto1, List} = read_(Proto0, list_begin),
    {Proto2, ok} = skip_list_loop(Proto1, List),
    {Proto3, ok} = read_(Proto2, list_end),
    {Proto3, ok};

skip(Proto0, Type) when is_atom(Type) ->
    {Proto1, _Ignore} = read_(Proto0, Type),
    {Proto1, ok}.


skip_struct_loop(Proto0) ->
    {Proto1, #protocol_field_begin{type = Type}} = read_(Proto0, field_begin),
    case Type of
        ?tType_STOP ->
            {Proto1, ok};
        _Else ->
            {Proto2, ok} = skip(Proto1, thrift_protocol:typeid_to_atom(Type)),
            {Proto3, ok} = read_(Proto2, field_end),
            skip_struct_loop(Proto3)
    end.

skip_map_loop(Proto0, #protocol_map_begin{size = 0}) ->
    {Proto0, ok};
skip_map_loop(Proto0, Map = #protocol_map_begin{ktype = Ktype, vtype = Vtype, size = Size}) ->
    {Proto1, ok} = skip(Proto0, Ktype),
    {Proto2, ok} = skip(Proto1, Vtype),
    skip_map_loop(Proto2, Map#protocol_map_begin{size = Size - 1}).

skip_set_loop(Proto0, #protocol_set_begin{size = 0}) ->
    {Proto0, ok};
skip_set_loop(Proto0, Map = #protocol_set_begin{etype = Etype, size = Size}) ->
    {Proto1, ok} = skip(Proto0, Etype),
    skip_set_loop(Proto1, Map#protocol_set_begin{size = Size - 1}).

skip_list_loop(Proto0, #protocol_list_begin{size = 0}) ->
    {Proto0, ok};
skip_list_loop(Proto0, Map = #protocol_list_begin{etype = Etype, size = Size}) ->
    {Proto1, ok} = skip(Proto0, Etype),
    skip_list_loop(Proto1, Map#protocol_list_begin{size = Size - 1}).

%%--------------------------------------------------------------------
%% Function: write(OProto, {Type, Data}) -> ok
%%
%% Type = {struct, StructDef} |
%%        {list, Type} |
%%        {map, KeyType, ValType} |
%%        {set, Type} |
%%        BaseType
%%
%% Data =
%%         tuple()  -- for struct
%%       | list()   -- for list
%%       | dictionary()   -- for map
%%       | set()    -- for set
%%       | any()    -- for base types
%%
%% Description:
%%--------------------------------------------------------------------
-spec write(#protocol{}, any()) -> {#protocol{}, ok | {error, _Reason}}.

write(Proto, TypeData) ->
    case validate(TypeData) of
        ok -> write_(Proto, TypeData);
        Error -> {Proto, Error}
    end.

write_(Proto0, {{struct, StructDef}, Data})
  when is_list(StructDef), is_tuple(Data), length(StructDef) == size(Data) - 1 ->

    [StructName | Elems] = tuple_to_list(Data),
    {Proto1, ok} = write_(Proto0, #protocol_struct_begin{name = StructName}),
    {Proto2, ok} = struct_write_loop(Proto1, StructDef, Elems),
    {Proto3, ok} = write_(Proto2, struct_end),
    {Proto3, ok};

write_(Proto, {{struct, {Module, StructureName}}, Data})
  when is_atom(Module),
       is_atom(StructureName),
       element(1, Data) =:= StructureName ->
    write_(Proto, {Module:struct_info(StructureName), Data});

write_(Proto, {{enum, Fields}, Data}) when is_list(Fields), is_atom(Data) ->
    {Data, IVal} = lists:keyfind(Data, 1, Fields),
    write_(Proto, {i32, IVal});

write_(Proto, {{enum, {Module, EnumName}}, Data})
  when is_atom(Module),
       is_atom(EnumName) ->
    write_(Proto, {Module:enum_info(EnumName), Data});

write_(Proto0, {{list, Type}, Data})
  when is_list(Data) ->
    {Proto1, ok} = write(Proto0,
               #protocol_list_begin{
                 etype = term_to_typeid(Type),
                 size = length(Data)
                }),
    Proto2 = lists:foldl(fun(Elem, ProtoIn) ->
                                 {ProtoOut, ok} = write_(ProtoIn, {Type, Elem}),
                                 ProtoOut
                         end,
                         Proto1,
                         Data),
    {Proto3, ok} = write(Proto2, list_end),
    {Proto3, ok};

write_(Proto0, {{map, KeyType, ValType}, Data}) ->
    {Proto1, ok} = write(Proto0,
                         #protocol_map_begin{
                           ktype = term_to_typeid(KeyType),
                           vtype = term_to_typeid(ValType),
                           size  = map_size(Data)
                          }),
    Proto2 = maps:fold(fun(KeyData, ValData, ProtoS0) ->
                               {ProtoS1, ok} = write_(ProtoS0, {KeyType, KeyData}),
                               {ProtoS2, ok} = write_(ProtoS1, {ValType, ValData}),
                               ProtoS2
                       end,
                       Proto1,
                       Data),
    {Proto3, ok} = write_(Proto2, map_end),
    {Proto3, ok};

write_(Proto0, {{set, Type}, Data}) ->
    true = ordsets:is_set(Data),
    {Proto1, ok} = write(Proto0,
                         #protocol_set_begin{
                           etype = term_to_typeid(Type),
                           size  = ordsets:size(Data)
                          }),
    Proto2 = ordsets:fold(fun(Elem, ProtoIn) ->
                               {ProtoOut, ok} = write_(ProtoIn, {Type, Elem}),
                               ProtoOut
                       end,
                       Proto1,
                       Data),
    {Proto3, ok} = write(Proto2, set_end),
    {Proto3, ok};

write_(Proto = #protocol{module = Module,
                        data = ModuleData}, Data) ->
    {NewData, Result} = Module:write(ModuleData, Data),
    {Proto#protocol{data = NewData}, Result}.

struct_write_loop(Proto0, [{Fid, Type} | RestStructDef], [Data | RestData]) ->
    NewProto = case Data of
                   undefined ->
                       Proto0; % null fields are skipped in response
                   _ ->
                       {Proto1, ok} = write_(Proto0,
                                           #protocol_field_begin{
                                             type = term_to_typeid(Type),
                                             id = Fid
                                            }),
                       {Proto2, ok} = write_(Proto1, {Type, Data}),
                       {Proto3, ok} = write_(Proto2, field_end),
                       Proto3
               end,
    struct_write_loop(NewProto, RestStructDef, RestData);
struct_write_loop(Proto, [], []) ->
    write_(Proto, field_stop).

%%

validate(#protocol_message_begin{}) -> ok;
validate(#protocol_struct_begin{}) -> ok;
validate(#protocol_field_begin{}) -> ok;
validate(#protocol_map_begin{}) -> ok;
validate(#protocol_list_begin{}) -> ok;
validate(#protocol_set_begin{}) -> ok;
validate(message_end) -> ok;
validate(field_end) -> ok;
validate(struct_end) -> ok;
validate(list_end) -> ok;
validate(set_end) -> ok;
validate(map_end) -> ok;

validate(TypeData) ->
    try validate(TypeData, []) catch
        throw:{invalid, Path, _Type, Value} ->
            {error, {invalid, get_path_literal(Path), Value}}
    end.

validate(TypeData, Path) ->
    validate(required, TypeData, Path).

validate(optional, {_Type, undefined}, _Path) ->
    ok;
validate(_Req, {{list, Type}, Data}, Path) when is_list(Data) ->
    lists:foreach(fun (E) -> validate({Type, E}, Path) end, Data);
validate(_Req, {{set, Type}, Data}, Path) when is_list(Data) ->
    lists:foreach(fun (E) -> validate({Type, E}, Path) end, (ordsets:to_list(Data)));
validate(_Req, {{map, KType, VType}, Data}, Path) when is_map(Data) ->
    maps:fold(fun (K, V, _) -> validate({KType, K}, Path), validate({VType, V}, Path), ok end, ok, Data);
validate(_Req, {{struct, {Mod, Name}}, Data}, Path) when is_tuple(Data) ->
    [_ | Elems] = tuple_to_list(Data),
    {struct, Types} = Mod:struct_info_ext(Name),
    validate_struct(Types, Elems, Path);
validate(_Req, {{struct, StructDef}, Data}, Path) when is_tuple(Data), is_list(StructDef) ->
    Elems = tuple_to_list(Data),
    if
        length(Elems) =:= length(StructDef) ->
            validate_union(StructDef, Elems, Path);
        true ->
            validate_union(StructDef, tl(Elems), Path)
    end;
validate(_Req, {{enum, _Fields}, Value}, _Path) when is_atom(Value), Value =/= undefined ->
    ok;

validate(_Req, {string, Value}, _Path) when is_binary(Value) -> ok;
validate(_Req, {bool, Value}, _Path) when is_boolean(Value) -> ok;
validate(_Req, {_Type, Value}, _Path) when is_number(Value) -> ok;
validate(_Req, {_Type, Value}, _Path) when is_number(Value) -> ok;
validate(_Req, {Type, Value}, Path) -> throw({invalid, Path, Type, Value}).

validate_struct(Types, Elems, Path) ->
    lists:foreach(
        fun ({{_, Req, Type, Name, _}, Data}) -> validate(Req, {Type, Data}, [Name | Path]) end,
        lists:zip(Types, Elems)
    ).

validate_union(Types, Elems, Path) ->
    lists:foreach(
        fun ({{_, Type}, Data}) -> validate(optional, {Type, Data}, Path) end,
        lists:zip(Types, Elems)
    ).

get_path_literal(Path) ->
    case lists:foldl(fun (E, A) -> [$. | atom_to_list(E) ++ A] end, "", Path) of
        [_ | V] -> V;
        [] -> []
    end.
