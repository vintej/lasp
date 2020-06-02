%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_delta_based_synchronization_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_synchronization_backend).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         get_members/1,
         time_stamp/0, 
         code_change/3]).

-export([propagate/1]).

%% lasp_synchronization_backend callbacks
-export([extract_log_type_and_payload/1]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store(), gossip_peers :: [], actor :: binary()}).

%%%===================================================================
%%% lasp_synchronization_backend callbacks
%%%===================================================================

%% delta_based messages:
extract_log_type_and_payload({delta_send, Node, {Id, Type, _Metadata, Deltas}, Counter}) ->
    [{Id, Deltas}, {Type, Deltas}, {delta_send, Deltas}, {delta_send_protocol, {Id, Node, Counter}}];
extract_log_type_and_payload({delta_ack, Node, Id, Counter}) ->
    [{delta_send_protocol, {Id, Node, Counter}}];
extract_log_type_and_payload({rate_class, Node, Rate}) ->
    [{delta_send_protocol, {Node, Rate}}];
extract_log_type_and_payload({rate_ack, Node, Rate}) ->
    [{delta_send_protocol, {Node, Rate}}];
extract_log_type_and_payload({rate_subscribe, Node, Rate}) ->
    [{delta_send_protocol, {Node, Rate}}];
extract_log_type_and_payload({find_sub, Node, Rate, Id}) ->
    [{delta_send_protocol, {Node, Rate, Id}}];
extract_log_type_and_payload({find_sub_aq, Id, ToNode, Node}) ->
    [{delta_send_protocol, {Id, ToNode, Node}}];
extract_log_type_and_payload({find_sub_aq_lock, Id, Node}) ->
    [{delta_send_protocol, {Id, Node}}].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

propagate(ObjectFilterFun) ->
    gen_server:call(?MODULE, {propagate, ObjectFilterFun}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([Store, Actor]) ->
    %% Seed the process at initialization.
    ?SYNC_BACKEND:seed(),
    
    ets:new(peer_rates, [ordered_set, named_table, public]),
    ets:insert(peer_rates, [{"self_rate", os:getenv("RATE_CLASS", "c1")}]),
    ets:new(rate_ack, [named_table, bag, public]),
    ets:new(c1, [named_table, bag, public]),
    ets:new(c2, [named_table, bag, public]),
    ets:new(c3, [named_table, bag, public]),
    ets:new(find_sub, [named_table, bag, public]),
    ets:new(find_sub_aq, [ named_table, bag, public]),
    lager:debug("LASPVIN test"),
    %schedule_delta_synchronization(),
    schedule_delta_garbage_collection(),
    schedule_rate_class_info_propagation(),
    schedule_rate_propagation_c1(),
    schedule_rate_propagation_c2(),
    schedule_rate_propagation_c3(),

    {ok, #state{actor=Actor, gossip_peers=[], store=Store}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({propagate, ObjectFilterFun}, _From, State) ->
    %% Get the active set from the membership protocol.
    {ok, Members} = ?SYNC_BACKEND:membership(),

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    %% Transmit updates.
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, ObjectFilterFun) end,
                  Peers),

    {reply, ok, State};


handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast({delta_exchange, Peer, ObjectFilterFun},
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_exchange"),

    lasp_logger:extended("Exchange starting for ~p", [Peer]),
    lager:debug("LASPVIN COming to delta_exch for Peer:~p ~n", [Peer]),

    Mutator = fun({Id, #dv{value=Value, type=Type, metadata=Metadata,
                           delta_counter=Counter, delta_map=DeltaMap,
                           delta_ack_map=AckMap0}=Object}) ->
        lager:debug("LASPVIN Inside mutator ~n"),
        case ObjectFilterFun(Id, Metadata) of
            true ->
                lager:debug("LASPVIN ObjectFilterFun True Id:~p Meta:~p ~n", [Id, Metadata]),
                Ack = case orddict:find(Peer, AckMap0) of
                    {ok, {Ack0, _GCCounter}} ->
                        Ack0;
                    error ->
                        0
                end,

                Min = lists_min(orddict:fetch_keys(DeltaMap)),

                Deltas = case orddict:is_empty(DeltaMap) orelse Min > Ack of
                    true ->
                        Value;
                    false ->
                        collect_deltas(Peer, Type, DeltaMap, Ack, Counter)
                end,

                ClientInReactiveMode =
                (?SYNC_BACKEND:client_server_mode() andalso
                 ?SYNC_BACKEND:i_am_client() andalso ?SYNC_BACKEND:reactive_server()),

                AckMap = case Ack < Counter orelse ClientInReactiveMode of
                    true ->
                        lager:debug("LASPVIN Ackmap True sending now ~n"),
                        ?SYNC_BACKEND:send(?MODULE, {delta_send, lasp_support:mynode(), {Id, Type, Metadata, Deltas}, Counter}, Peer),

                        orddict:map(
                            fun(Peer0, {Ack0, GCCounter0}) ->
                                case Peer0 of
                                    Peer ->
                                        {Ack0, GCCounter0 + 1};
                                    _ ->
                                        {Ack0, GCCounter0}
                                end
                            end,
                            AckMap0
                        );
                    false ->
                        lager:debug("LASPVIN AckMap False skipping~n"),
                        AckMap0
                end,

                {Object#dv{delta_ack_map=AckMap}, Id};
            false ->
                lager:debug("LASPVIN ObjectFilterFun False Id:~p Meta:~p ~n", [Id, Metadata]),
                {Object, skip}
        end
    end,

    %% TODO: Should this be parallel?
    {ok, _} = lasp_storage_backend:update_all(Store, Mutator),

    lasp_logger:extended("Exchange finished for ~p", [Peer]),
    lager:debug("Exchange finished~n"),

    {noreply, State};

handle_cast({delta_send, From, {Id, Type, _Metadata, Deltas}, Counter},
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_send"),

    {Time, _Value} = timer:tc(fun() ->
                    ?CORE:receive_delta(Store, {delta_send,
                                                From,
                                               {Id, Type, _Metadata, Deltas},
                                               ?CLOCK_INCR(Actor),
                                               ?CLOCK_INIT(Actor)})
             end),
    lasp_logger:extended("Receiving delta took: ~p microseconds.", [Time]),
    lager:error("LASPVIN Received delta From=~p at TimeStamp=~p Took=~p microseconds ~n", [From, time_stamp(), Time]),

    %% Acknowledge message.
    ?SYNC_BACKEND:send(?MODULE, {delta_ack, lasp_support:mynode(), Id, Counter}, From),

    %% Send back just the updated state for the object received.
    case ?SYNC_BACKEND:client_server_mode() andalso
         ?SYNC_BACKEND:i_am_server() andalso ?SYNC_BACKEND:reactive_server() of
        true ->
            ObjectFilterFun = fun(Id1, _) ->
                                      Id =:= Id1
                              end,
            init_delta_sync(From, ObjectFilterFun);
        false ->
            ok
    end,

    {noreply, State};

handle_cast({delta_ack, From, Id, Counter}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_ack"),

    ?CORE:receive_delta(Store, {delta_ack, Id, From, Counter}),
    {noreply, State};

handle_cast({find_sub_aq, Id, ToNode, From}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub_aq"),
    lager:debug("LASPVIN Store ~p ~n",[Store]),
    lager:debug("LASPVIN received find_sub_aq for Id:~p From:~p ~n", [Id, From]),
    case ets:member(find_sub_aq, Id) of
        true ->
            case ets:member(peer_rates, ToNode) of
                true -> lager:error("LASPVIN ToNode ~p is a Peer.. Skipping ~n", [ToNode]), ok;
                false -> 
                    case lists:member(ToNode, ets:lookup_element(c1, "pseudopeer", 2)) of
                        true -> lager:error("LASPVIN path ToNode: ~p exists ~n",[ToNode]);
                        false -> found_sub_aq_lockpath(Id, ToNode, From)
                    end
            end;
        false -> found_sub_aq_lockpath(Id, ToNode, From)
    end,
    {noreply, State};

handle_cast({find_sub_aq_lock, Id, From}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub_aq_lock"),
    lager:debug("LASPVIN Store ~p ~n",[Store]),
    lager:debug("LASPVIN received find_sub_aq_lock for Id:~p From:~p ~n", [Id, From]),
    case ets:lookup_element(peer_rates, "self_rate", 2)==lists:nth(1,lists:nth(1,ets:match(find_sub, {'$1',Id, '_' }))) of
        true ->
            lager:error("LASPVIN Rate updated already ~n"),
            forward_aq_lock(Id);
        false ->
            lager:debug("LASPVIN updating rate ~n"),
            ets:update_element(peer_rates, "self_rate", {2, lists:nth(1, lists:nth(1,ets:match(find_sub, {'$1', Id, '_'})))}),
            %Update scubscription if not already subscribed to c1
            %ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]),
            %?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
            %propagate update_rate for all? may be at the end of the function,
            forward_aq_lock(Id),
            ets:delete_all_objects(rate_ack)
    end,
    %ets:delete_object(find_sub, lists:nth(1,ets:match_object(find_sub, {'_', Id, '_'}))),
    {noreply, State};

handle_cast({find_sub, From, ReqRate, Id}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub"),
    lager:debug("LASPVIN store:~p ~n", [Store]),
    lager:error("LASPVIN received find_sub Id: ~p From: ~p ~n", [Id, From]),
    case ets:member(find_sub_aq, Id) of
        true ->
            ok;
        false ->
            check_sub_exists(From, ReqRate, Id),
            case ets:member(find_sub_aq, Id) of
                true ->
                    case ReqRate of
                        "c1" -> forward_sub_req(Id);
                        "c2" -> lager:error("LASPVIN Skip forwarding for class c2")
                    end;
                false -> lager:error("LASPVIN Request forwarded~n")
            end
    end,
    {noreply, State};

handle_cast({rate_ack, From, Rate}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_ack"),
    lager:debug("LASPVIN received ack from ~p Store ~p ~n", [From, Store]),
    case Rate ==  ets:lookup_element(peer_rates, "self_rate", 2) of
       true -> ets:insert(rate_ack, [{From}]);
       false -> ok
    end,
    {noreply, State};

handle_cast({rate_class, From, Rate}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_class"),

    ?CORE:receive_delta(Store, {rate_class, From, Rate}),
    lager:debug("LASPVIN received rate_class From:~p rate:~p Store:~p", [From, Rate, Store]),
    case ets:member(peer_rates, From) of
       true -> 
          case ets:lookup_element(peer_rates, From, 2) == Rate of
             true -> ok;
             false ->
                peer_rate_update(From, Rate, ets:lookup_element(peer_rates, From, 2))
          end;
       false -> 
          ets:insert(peer_rates, [{From, Rate}]),
          peer_rate_insert(From, Rate)
    end,
    lager:debug("LASPVIN peer_rates updated list: ~p ~n",[ets:tab2list(peer_rates)]),
    lager:debug("LASPVIN c1 list: ~p ~n", [ets:tab2list(c1)]),
    lager:debug("LASPVIN c2 list: ~p ~n", [ets:tab2list(c2)]),
    lager:debug("LASPVIN c3 list: ~p ~n", [ets:tab2list(c3)]),
    ?SYNC_BACKEND:send(?MODULE, {rate_ack, lasp_support:mynode(), Rate}, From),
    case ets:first(find_sub) of
       '$end_of_table' -> ok;
       _Else ->
          %Send Find_sub req
          case ets:member(find_sub, Rate) of
              true ->
                  lists:foreach(fun(Id) ->
                      found_sub(Id, From)
                    end,
                ets:lookup_element(find_sub, Rate, 2));
              false ->
                  forward_find_sub_on_join(From)
            end
    end,
    {noreply, State};

handle_cast({rate_subscribe, From, Rate}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_subscribe"),

    ?CORE:receive_delta(Store, {rate_class, From, Rate}),
    lager:debug("LASPVIN received rate_subscribe From:~p rate:~p Store:~p", [From, Rate, Store]),
    case Rate of
             "c1" -> 
                 case check_member_list(c1, From, "subscriber") of
                    true -> ok;
                    false -> ets:insert(c1, [{"subscriber", From}])
                 end;
             "c2" -> 
                 case check_member_list(c2, From, "subscriber") of
                    true -> ok;
                    false -> ets:insert(c2, [{"subscriber", From}])
                 end;
             "c3" ->
                 case check_member_list(c3, From, "subscriber") of
                    true -> ok;
                    false -> ets:insert(c3, [{"subscriber", From}])
                 end
    end,
    lager:debug("LASPVIN c1 list: ~p ~n", [ets:tab2list(c1)]),
    lager:debug("LASPVIN c2 list: ~p ~n", [ets:tab2list(c2)]),
    lager:debug("LASPVIN c3 list: ~p ~n", [ets:tab2list(c3)]),
    {noreply, State};

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

handle_info(delta_sync, #state{}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_sync"),

    lasp_logger:extended("Beginning delta synchronization."),

    %% Get the active set from the membership protocol.
    {ok, Members} = ?SYNC_BACKEND:membership(),

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    lasp_logger:extended("Beginning sync for peers: ~p", [Peers]),

    %% Ship buffered updates for the fanout value.
    FilterWithoutConvergenceFun = fun(Id, _) ->
                              Id =/= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, FilterWithoutConvergenceFun) end,
                  Peers),

    %% Synchronize convergence structure.
    FilterWithConvergenceFun = fun(Id, _) ->
                              Id =:= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, FilterWithConvergenceFun) end,
                  ?SYNC_BACKEND:without_me(Members)),

    %% Schedule next synchronization.
    schedule_delta_synchronization(),

    {noreply, State#state{}};

handle_info(delta_gc, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_gc"),

    MaxGCCounter = lasp_config:get(delta_mode_max_gc_counter,
                                   ?MAX_GC_COUNTER),

    %% Generate garbage collection function.
    Mutator = fun({Id, #dv{delta_map=DeltaMap0,
                           delta_ack_map=AckMap0}=Object}) ->

        %% Only keep in the ack map nodes with gc counter
        %% below `MaxGCCounter'.
        PruneFun = fun(_Node, {_Ack, GCCounter}) ->
            GCCounter < MaxGCCounter
        end,

        PrunedAckMap = orddict:filter(PruneFun, AckMap0),

        %% Determine the min ack present in the ack map
        MinAck = lists_min([Ack || {_Node, {Ack, _GCCounter}} <- PrunedAckMap]),

        %% Remove unnecessary deltas from the delta map
        DeltaMapGCFun = fun(Counter, {_Origin, _Delta}) ->
            Counter >= MinAck
        end,

        DeltaMapGC = orddict:filter(DeltaMapGCFun, DeltaMap0),

        {Object#dv{delta_map=DeltaMapGC, delta_ack_map=PrunedAckMap}, Id}
    end,

    {ok, _} = lasp_storage_backend:update_all(Store, Mutator),

    %% Schedule next GC and reset counter.
    schedule_delta_garbage_collection(),

    {noreply, State};

handle_info(rate_info, #state{store=Store}=State) ->

    %% Get the active set from the membership protocol.
    {ok, Members} = ?SYNC_BACKEND:membership(),

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),
    lager:debug("LASPVIN Store rate_info ~p ~n", [Store]), 
    %% Transmit rate.
    lists:foreach(fun(Peer) ->
                        case ets:member(rate_ack, Peer) of
                           true -> ok;
                           false -> ?SYNC_BACKEND:send(?MODULE, {rate_class, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, Peer)
                        end
                  end, 
                  Peers),
    check_subscription(),
    schedule_rate_class_info_propagation(),
    {noreply, State};

handle_info(node_c1, #state{store=Store}=State) ->

    %% Get the active set from the membership protocol.
    {ok, Members} = ?SYNC_BACKEND:membership(),

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),
    lager:debug("LASPVIN Store rate_info ~p ~n", [Store]), 
    %% Transmit updates.
    lists:foreach(fun(Peer) ->
                        case ets:member(rate_ack, Peer) of
                           true -> ok;
                           false -> ?SYNC_BACKEND:send(?MODULE, {rate_class, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, Peer)
                        end
                  end, 
                  Peers),
    check_subscription(),
    schedule_rate_class_info_propagation(),
    {noreply, State};

handle_info(rate_prop_c1, #state{store=Store}=State) ->
    lager:debug("LASPVIN Store: ~p State:~p ~n", [Store, State]),
    case ets:member(c1, "peer") of
        true -> propagate_by_class(c1, "peer");
        false -> lager:debug("LASPVIN no c1 peers for propagation")
    end,
    schedule_rate_propagation_c1(),
    {noreply, State};

handle_info(rate_prop_c2, #state{store=Store}=State) ->
    lager:debug("LASPVIN Store: ~p State:~p ~n", [Store, State]),
    case ets:member(c2, "subscriber") of
        true -> propagate_by_class(c2, "subscriber");
        false -> lager:debug("LASPVIN no c2 subscriber for propagation")
    end,
    schedule_rate_propagation_c2(),
    {noreply, State};


handle_info(rate_prop_c3, #state{store=Store}=State) ->
    lager:debug("LASPVIN Store: ~p State:~p ~n", [Store, State]),
    case ets:member(c3, "subscriber") of
        true -> propagate_by_class(c3, "subscriber");
        false -> lager:debug("LASPVIN no c3 subscriber for propagation")
    end,
    schedule_rate_propagation_c3(),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
collect_deltas(Peer, Type, DeltaMap, PeerLastAck, DeltaCounter) ->
    orddict:fold(
        fun(Counter, {Origin, Delta}, Deltas) ->
            case (Counter >= PeerLastAck) andalso
                 (Counter < DeltaCounter) andalso
                 Origin /= Peer of
                true ->
                    lasp_type:merge(Type, Deltas, Delta);
                false ->
                    Deltas
            end
        end,
        lasp_type:new(Type),
        DeltaMap
    ).

%% @private
schedule_delta_synchronization() ->
    ShouldDeltaSync = true
            andalso (
              ?SYNC_BACKEND:peer_to_peer_mode()
              orelse
              (
               ?SYNC_BACKEND:client_server_mode()
               andalso
               not (?SYNC_BACKEND:i_am_server() andalso ?SYNC_BACKEND:reactive_server())
              )
            ),

    case ShouldDeltaSync of
        true ->
            Interval = lasp_config:get(delta_interval, 10000),
            case lasp_config:get(jitter, false) of
                true ->
                    %% Add random jitter.
                    Jitter = rand:uniform(Interval),
                    timer:send_after(Interval + Jitter, delta_sync);
                false ->
                    timer:send_after(Interval, delta_sync)
            end;
        false ->
            ok
    end.

get_members(ListToGet) ->
    ets:tab2list(ListToGet).

%% @private
peer_rate_insert(From, Rate) ->
    case Rate of
             "c1" ->
                 case check_member_list(c1, From, "peer") of
                    true -> ok;
                    false -> ets:insert(c1, [{"peer", From}])
                 end;
             "c2" ->
                 case check_member_list(c2, From, "peer") of
                    true -> ok;
                    false -> ets:insert(c2, [{"peer", From}])
                 end;
             "c3" ->
                 case check_member_list(c3, From, "peer") of
                    true -> ok;
                    false -> ets:insert(c3, [{"peer", From}])
                 end
    end.


time_stamp() ->
    {_, _, Micro} = erlang:timestamp(),
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_datetime(erlang:now()),
    lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.~3..0s",[Year,Month,Day,Hour,Minute,Second, integer_to_list(Micro)])).

%% @private
peer_rate_update(From, NewRate, OldRate) ->
    case OldRate of
        "c1" -> ets:delete_object(c1, {"peer", From});
        "c2" -> ets:delete_object(c2, {"peer", From});
        "c3" -> ets:delete_object(c3, {"peer", From})
    end,
    peer_rate_insert(From, NewRate),
    ets:update_element(peer_rates, From, {2, NewRate}).

%% @private
schedule_delta_garbage_collection() ->
    timer:send_after(?DELTA_GC_INTERVAL, delta_gc).

%% @private
schedule_rate_class_info_propagation() ->
    lager:debug("LASPVIN test"),
    timer:send_after(10000, rate_info).

%% @private
schedule_rate_propagation_c1() ->
    lager:debug("LASPVIN rate_propagation_c1"),
    %5000 milliseconds is 5 seconds
    timer:send_after(5000, rate_prop_c1).

%% @private
schedule_rate_propagation_c2() ->
    lager:debug("LASPVIN rate_propagation_c2"),
    %22500 milliseconds is 22.5 seconds
    timer:send_after(22500, rate_prop_c2).

%% @private
schedule_rate_propagation_c3() ->
    lager:debug("LASPVIN rate_propagation_c3"),
    %22500 milliseconds is 22.5 seconds
    timer:send_after(22500, rate_prop_c3).

%% @private
propagate_by_class(Class, Sub) ->
    lasp_logger:extended("Beginning delta synchronization by class."),

    
    %% Ship buffered updates for the fanout value.
    FilterWithoutConvergenceFun = fun(Id, _) ->
                              Id =/= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, FilterWithoutConvergenceFun) end,
                  get_subscribers(Class, Sub)),

    %% Synchronize convergence structure.
    FilterWithConvergenceFun = fun(Id, _) ->
                              Id =:= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, FilterWithConvergenceFun) end,
                  get_subscribers(Class, Sub)).

%% @private
get_subscribers(Class, Sub) ->
    ets:lookup_element(Class, Sub, 2).

%% @private
init_delta_sync(Peer, ObjectFilterFun) ->
    gen_server:cast(?MODULE, {delta_exchange, Peer, ObjectFilterFun}).

%% @private
check_member_list(RateList, Member, Role) ->
    case ets:member(RateList, Role) of
       true -> lists:member(Member, ets:lookup_element(RateList, Role, 2));
       false -> ets:member(RateList, Role)
    end.

%% @private
get_peers() ->
    {ok, Members} = ?SYNC_BACKEND:membership(),
    %% Remove ourself and compute exchange peers.
    ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)).

%% @private
check_subscription() ->
    case ets:member(peer_rates, "subscription") of
       true -> lager:debug("LASPVIN subscription done already ~n"),ok;
       false ->
          case ets:lookup_element(peer_rates, "self_rate", 2) > "c1" of
             true ->
                case ets:lookup_element(peer_rates, "self_rate", 2) < "c3" of
                   true ->
                      case ets:member(c1, "peer") of
                         true -> ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]), ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                         false -> 
                            io:fwrite("LASPVIN no peer to subscribe Case 1 ~n ")
                      end;
                   false ->
                      case ets:member(c2, "peer") of
                         true ->  ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c2, "peer", 2))}]), ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c2, "peer", 2)));
                         false ->
                            case ets:member(c1, "peer") of
                               true -> ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]), ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                               false -> io:fwrite("LASPVIN no peer to subscribe case 2 ~n")
                            end
                      end
                end;
             false ->
                case ets:member(c1, "peer") of
                   true -> ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]), ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                   false ->
                      case ets:member(find_sub, ets:lookup_element(peer_rates, "self_rate", 2)) of
                         true -> lager:debug("Find_sub_req exists for the class");
                         false -> 
                            ets:insert(find_sub, [{"c1", erlang:atom_to_list(lasp_support:mynode())++"c1", lasp_support:mynode()}]), 
                            forward_sub_req(erlang:atom_to_list(lasp_support:mynode())++"c1")
                      end
                end
          end
    end.

%%private
forward_sub_req(Id) ->
   lager:debug("LASPVIN no c1 peer to subscribe forwarding to peers ~n"),
   lists:foreach(fun(Peer) ->
      case lists:member(Peer, ets:lookup_element(find_sub, "c1", 3)) of
         true -> ok;
         false ->
             case erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)) == Peer of
               true -> lager:error("LASPVIN Peer ~p is Source of Req.. Skipping ~n",[Peer]);
               false -> ?SYNC_BACKEND:send(?MODULE, {find_sub, lasp_support:mynode(),"c1", Id}, Peer)
            end
      end
   end,
   get_peers()).

%% @private
check_sub_exists(From, ReqRate, Id) ->
    case ets:member(find_sub, ReqRate) of
       true ->
          case lists:member(Id, ets:lookup_element(find_sub, ReqRate, 2)) of
             true -> lager:debug("LASPVIN Find_sub request id exists");
             false -> 
                lager:debug("LASPVIN Matching find_sub rates found~n"),
                lager:debug("LASPVIN find_sub:insert ReqRate:~p Id:~p From:~p ~n", [ReqRate, Id, From]),
                ets:insert(find_sub, [{ReqRate, Id, From}]),
                found_sub(Id, erlang:list_to_atom(string:sub_string(lists:nth(1,ets:lookup_element(find_sub, ReqRate, 2)), 1, string:len(lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)))-2)))
          end;
       false ->
          ets:insert(find_sub, {ReqRate, Id, From}),
          lager:debug("LASPVIN test2 coming here 1"),
          case ReqRate of
             "c1" ->
                lager:debug("LASPVIN test2 coming here 2"),
                case ets:member(c1, "peer") of
                   true ->
                      lager:debug("LASPVIN test2 coming here 3"),
                      case lists:member(From, ets:lookup_element(c1, "peer", 2)) of
                         true -> 
                            case length(ets:lookup_element(c1, "peer", 2)) > 1 of
                               true -> 
                                   lager:debug("LASPVIN I found the peer ~n"),
                                   case lists:nth(1, ets:lookup_element(c1, "peer", 2)) == From of
                                       true -> found_sub(Id, lists:nth(2, ets:lookup_element(c1, "peer", 2)));
                                       false -> found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
                                    end;
                               false -> lager:debug("LASPVIN forward request to peers"), forward_sub_req(Id)
                            end;
                         false ->
                             found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
                      end;
                   false -> lager:debug("LASPVIN send to peers"), forward_sub_req(Id)
                end;
             "c2" ->
                case ets:member(c2, "peer") of
                   true ->
                      case lists:member(From, ets:lookup_element(c2, "peer", 2)) of
                         true -> 
                            case length(ets:lookup_element(c2, "peer", 2)) > 1 of
                               true ->
                                   case lists:nth(1, ets:lookup_element(c2, "peer", 2)) == From of
                                       true -> found_sub(Id, lists:nth(2, ets:lookup_element(c2, "peer", 2)));
                                       false -> found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
                                    end;
                               false -> lager:debug("LASPVIN forward request to peers ~n"), forward_sub_req(Id)
                            end;
                         false ->
                             found_sub(Id, lists:nth(1, ets:lookup_element(c2, "peer", 2)))
                      end;
                   false -> 
                      case ets:member(c1, "peer") of
                         true -> 
                             found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                         false -> lager:debug("LASPVIN send to peers"), forward_sub_req(Id)
                      end
                end
          end
    end.

%%private
forward_find_sub_on_join(From) ->
    lists:foreach(fun(ReqRate) ->
                          case From == lists:nth(1,ets:lookup_element(find_sub, lists:nth(1,ReqRate), 3)) of
                             true -> ok;
                             false ->lager:debug("LASPVIN sent find_sub req ~n"), ?SYNC_BACKEND:send(?MODULE, {find_sub, lasp_support:mynode(), lists:nth(1,ReqRate), lists:nth(1,ets:lookup_element(find_sub, lists:nth(1,ReqRate), 2))}, From)
                          end
                        end,
                  lists:usort(ets:match(find_sub, {'$1', '_', '_'}))).

%%private
found_sub(Id, ToNode) ->
    case erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)) == ToNode of
        true -> lager:error("LASPVIN False call");
        false ->
            lager:error("LASPVIN found the peer at ~p for ID: ~p ToNode: ~p ~n", [time_stamp(), Id, ToNode]),
            case ets:member(find_sub_aq, Id) of
                true -> ok;
                false ->
                    ets:insert(find_sub_aq, [{Id, ToNode, lasp_support:mynode()}]),
                    %timer:sleep(5),
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, lasp_support:mynode()}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1'}))))
            end
    end.


%%private
found_sub_aq_lockpath(Id, ToNode, From) ->
    case lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1'}))) == lasp_support:mynode() of
                true ->
                    lager:error("LASPVIN Got path to ~p ID:~p ~n", [ToNode, Id]),
                    ets:insert(find_sub_aq, [{Id, ToNode, From}]),
                    ets:insert(c1, [{"pseudopeer", ToNode}]),
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock, Id, lasp_support:mynode()}, From);
                false -> 
                    ets:insert(find_sub_aq, [{Id, ToNode, From}]),
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, lasp_support:mynode()}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1'}))))
    end.

%%private
forward_aq_lock(Id) ->
    case ets:lookup_element(find_sub_aq, Id, 2) == lasp_support:mynode() of
                true ->
                    %check find_sub if there are any other nodes requiring same rate,
                    %if there are inform them 
                    lager:error("LASPVIN Locking reached chain end");
                false ->
                    %pass on the lock & delete find_sub_aq entry
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock, Id, lasp_support:mynode()},ets:lookup_element(find_sub_aq, Id, 2))
                    %ets:delete(find_sub_aq, Id)
    end.

%% @private
lists_min([]) -> 0;
lists_min(L) -> lists:min(L).
