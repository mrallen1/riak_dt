%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% a G-Set CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_dt_gset).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

%% EQC generator
-ifdef(EQC).
gen_op() ->
    {add, int()}.

init_state() ->
    ordsets:new().

update_expected(_ID, {add, Elem}, Prev) ->
    ordsets:add_element(Elem, Prev);
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    ordsets:to_list(S).

-endif.

new() ->
    ordsets:new().

value(GSet) ->
    ordsets:to_list(GSet).

update({add, Elem}, _Actor, GSet) ->
    ordsets:add_element(Elem, GSet).

merge(GSet1, GSet2) ->
    ordsets:union(GSet1, GSet2).

equal(GSet1, GSet2) ->
    GSet1 == GSet2.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.
-endif.
-endif.
