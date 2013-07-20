%% -------------------------------------------------------------------
%%
%% riak_dt_cbmap: OR-Set schema based Name->CRDT map
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_dt_cbmap).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, keys/1, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, generate/0]).
-endif.

new() ->
    {sets:new(), sets:new()}.

value({Add, _Remove}) ->
    sets:fold(fun({{Name, Type}, V, _U}, Acc) ->
                        orddict:update(Name, fun(Current) ->
                                                  Type:merge(Current, V) end, V, Acc) end,
                orddict:new(),
                Add).

value({Name, Type}=Key, {Add, _Remove}) ->
    Acc0 = orddict:new(),
    Acc1 = orddict:store(Name, Type:new(), Acc0),
    sets:fold(fun({K, V, _U}, Acc) when K =:= Key ->
                        orddict:update(Name, fun(Current) ->
                                                  Type:merge(Current, V) end, V, Acc);
                  (_, Acc) -> Acc end,
              Acc1,
              Add).

keys({Add, _Remove}) ->
    sets:fold(fun({K, _V, _U}, Acc) ->
                      sets:add_element(K, Acc) end,
              sets:new(),
              Add).

update({add, {Name, Type}=Key, CRDT}, Actor, {Add, Remove}) ->
    Tag = unique(Actor),
    [{Name, Current}] = value(Key, {Add, Remove}),
    ToAdd = Type:merge(Current, CRDT),
    {Add2, Remove2} = remove(Key, {Add, Remove}),
    {sets:add_element({Key, ToAdd, Tag}, Add2), Remove2};
update({put, {_Name, _Type}=Key, CRDT}, Actor, {Add, Remove}) ->
    Tag = unique(Actor),
    {Add2, Remove2} = remove(Key, {Add, Remove}),
    {sets:add_element({Key, CRDT, Tag}, Add2), Remove2};
update({remove, {_Name, _Type}=Key}, _Actor, {Add, Remove}) ->
    remove(Key, {Add, Remove}).

%% @Private
remove(Key, {Add, Remove}) ->
    ToRem = sets:fold(fun({K, _V, _U}=Elem, Acc) when K=:=Key->
                              sets:add_element(Elem, Acc);
                         (_, Acc) -> Acc end,
                      sets:new(),
                      Add),
    {sets:subtract(Add, ToRem), sets:union(Remove, ToRem)}.

unique(Actor) ->
    erlang:phash2({Actor, erlang:now()}).

merge({Add1, Remove1}, {Add2, Remove2}) ->
    Remove3 = sets:union(Remove1, Remove2),
    PreAdd3 = sets:union(Add1, Add2),
    Add3 = sets:subtract(PreAdd3, Remove3),
    {Add3, Remove3}.

equal({Add1, Remove1}, {Add2, Remove2}) ->
    sets:is_subset(Add1, Add2) andalso sets:is_subset(Add2, Add1)
        andalso sets:is_subset(Remove1, Remove2) andalso
        sets:is_subset(Remove2, Remove1).

to_binary(Map) ->
    term_to_binary(Map).

from_binary(Bin) ->
    binary_to_term(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {insert, Field, gen_field_insert(Field)},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {binary(), oneof([riak_dt_pncounter, riak_dt_vvorset, riak_dt_lwwreg,
                       riak_dt_gcounter, riak_dt_zorset, riak_dt_cbmap])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

gen_field_insert({_Name, Type}) ->
     Type:generate().

generate() ->
    ?LET(Fields, list(gen_field()),
         begin
             Values = [{F, Type:generate()} || {_Name, Type}=F <- Fields, Type /= riak_dt_cbmap], %% @TODO fix this, deadlocks otherwise
             lists:foldl(fun({K, V}, {FSet, VDict}) ->
                                 Fields2 = riak_dt_vvorset:update({add, K}, choose(1, 50), FSet),
                                 Values2 = orddict:store(K, V, VDict),
                                 {Fields2, Values2} end,
                         riak_dt_multi:new(),
                         Values)
                 end).

init_state() ->
    {0, dict:new()}.

update_expected(ID, {update, Ops}, Values) ->
    lists:foldl(fun(Op, V) ->
                        update_expected(ID, Op, V) end,
                Values,
                Ops);

update_expected(ID, {add, {_Name, Type}=Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Type:new(), Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, V, X} || {E, V, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
update_expected(ID, {update, {_Name, Type}=Key, Op}, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID,  Dict),
    Updated = Type:update(Op, ID, CurrentValue),
    Cnt = Cnt0+1,
    ToAdd = {Key, Updated, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {insert, {_Name, Type}=Key, Value}, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID,  Dict),
    Updated = Type:merge(Value, CurrentValue),
    Cnt = Cnt0+1,
    ToAdd = {Key, Updated, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)}.

eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

%% On shrinking this fails with some badarge shit
%% {'EXIT',
%%     {badarg,
%%         [{dict,fetch,
%%              [5,
%%               {dict,1,16,16,8,80,48,
%%                   {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%                   {{[[0|
%%                       {{set,0,16,16,8,80,48,
%%                            {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%                            {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
%%                              []}}},
%%                        {set,0,16,16,8,80,48,
%%                            {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%                            {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
%%                              []}}}}]],
%%                     [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}],
%%              [{file,"dict.erl"},{line,125}]},
%%          {riak_dt_multi,get_for_key,3,
%%              [{file,"src/riak_dt_multi.erl"},{line,307}]},
get_for_key({_N, T}=K, ID, Dict) ->
    {A, R} = dict:fetch(ID, Dict),
    Remaining = sets:subtract(A, R),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    proplists:get_value(K, dict:to_list(Res), T:new()).

-endif.

query_test() ->
    Map = new(),
    Map1 = update({update, [{add, {c, riak_dt_pncounter}},
                            {add, {s, riak_dt_vvorset}},
                            {add, {m, riak_dt_multi}},
                            {add, {l, riak_dt_lwwreg}},
                            {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    ?assertEqual(5, value(size, Map1)),

    Map2 = update({update, [{remove, {l2, riak_dt_lwwreg}}]}, a2, Map1),
    ?assertEqual(4, value(size, Map2)),

    ?assertEqual([{c, riak_dt_pncounter},
                  {l, riak_dt_lwwreg},
                  {m, riak_dt_multi},
                  {s, riak_dt_vvorset}], value(keyset, Map2)),

    ?assert(value({contains, {c, riak_dt_pncounter}}, Map2)),
    ?assertNot(value({contains, {l2, riak_dt_lwwreg}}, Map2)),

    Map3 = update({update, [{update, {c, riak_dt_pncounter}, {increment, 33}},
                            {update, {l, riak_dt_lwwreg}, {assign, lww_val, 77}}]}, a3, Map2),

    ?assertEqual(33, value({get, {c, riak_dt_pncounter}}, Map3)),
    ?assertEqual({lww_val, 77}, value({get_crdt, {l, riak_dt_lwwreg}}, Map3)).

-endif.
