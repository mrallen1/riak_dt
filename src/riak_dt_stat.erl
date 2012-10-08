%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_dt_stat).

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/0, update/1,
         register_stats/0, produce_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-define(APP, riak_dt).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    [(catch folsom_metrics:delete_metric({?APP, Name})) || {Name, _Type} <- stats()],
    [register_stat({?APP, Name}, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    [stat_value(Stat, Type) || {{App, Stat}, [{type, Type}]} <- folsom_metrics:get_metrics_info(), App == ?APP].

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    update1(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update1({coord_redir, Node}) ->
    folsom_metrics:notify_existing_metric({?APP, coord_redir_tot}, {inc, 1}, counter),
    StatName = join(coord_redir, Node),
    case (catch folsom_metrics:notify_existing_metric({?APP, StatName}, {inc, 1}, counter)) of
        ok ->
            ok;
        {'EXIT', _} ->
            register_stat({?APP, StatName}, counter),
            folsom_metrics:notify_existing_metric({?APP, StatName}, {inc, 1}, counter)
    end.

%% private
stats() ->
    [{coord_redir_tot, counter}].

register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, duration) ->
    folsom_metrics:new_duration(Name).

stat_value(Name, histogram) ->
    {Name, folsom_metrics:get_histogram_statistics({?APP, Name})};
stat_value(Name, _Type) ->
    {Name, folsom_metrics:get_metric_value({?APP, Name})}.

join(Atom1, Atom2) ->
    Bin1 = atom_to_binary(Atom1, latin1),
    Bin2 = atom_to_binary(Atom2, latin1),
    binary_to_atom(<<Bin1/binary, $_, Bin2/binary>>, latin1).
