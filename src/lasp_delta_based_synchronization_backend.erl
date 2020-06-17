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
extract_log_type_and_payload({find_sub, Node, Rate, Id, Hop}) ->
    [{delta_send_protocol, {Node, Rate, Id, Hop}}];
extract_log_type_and_payload({find_sub_aq, Id, ToNode, ViaNode, Node, Hop}) ->
    [{delta_send_protocol, {Id, ToNode, ViaNode, Node, Hop}}];
extract_log_type_and_payload({find_sub_aq_lock, Id, ToNode, Node}) ->
    [{delta_send_protocol, {Id, ToNode, Node}}];
extract_log_type_and_payload({check_tonode, ToNode, Hop, Node}) ->
    [{delta_send_protocol, {ToNode, Hop, Node}}];
extract_log_type_and_payload({sub_cancel, Node, Id, ToNode}) ->
    [{delta_send_protocol, {Node, Id, ToNode}}];
extract_log_type_and_payload({find_sub_aq_lock_rev, Id, Node}) ->
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
    ets:new(rate_ack, [named_table, ordered_set, public]),
    ets:new(temp_tonode, [named_table, bag, public]),
    ets:new(match_sub_aq, [named_table, bag, public]),
    ets:new(c1, [named_table, bag, public]),
    ets:new(c2, [named_table, bag, public]),
    ets:new(c3, [named_table, bag, public]),
    ets:new(find_sub, [named_table, bag, public]),
    ets:new(find_sub_aq, [ named_table, bag, public]),
    ets:new(myconnections, [named_table, bag, public]),
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
                        lager:error("LASPVIN Sending delta to ~p ~n", [Peer]),
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


handle_cast({find_sub_aq, Id, ToNode, Via, From, Hop}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub_aq"),
    lager:debug("LASPVIN Store ~p ~n",[Store]),
    lager:error("LASPVIN received find_sub_aq for Id:~p ToNode:~p Via:~p From:~p HopCount:~p ~n", [Id, ToNode, Via, From, Hop]),
    lager:error("At receving find_sub_aq for Id ~p, find_sub_aq:~p ~n", [Id, ets:tab2list(find_sub_aq)]),
    case ToNode == lasp_support:mynode() of
        true -> lager:error("Discarding is I am the ToNode for Id ~p ~n ",[Id]);
        false ->
            case ets:member(find_sub_aq, Id) of
                true ->
                    case ets:member(peer_rates, ToNode) of
                        true -> 
                            case From==Via of
                                true ->
                                    case lists:member([ToNode], ets:match(find_sub_aq, {'_', '$1', '_', '_'})) of
                                        true -> 
                                            lager:error("LASPVIN path ToNode: ~p exists find_sub_aq:~p ~n",[ToNode, ets:match(find_sub_aq, {'_', ToNode, '_', '$1'})]),
                                            case Hop < lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))) of
                                                true ->
                                                    lager:error("Got new path for a peer with lower hopcount ~p ToNode ~p  From ~p for Id ~p Via ~p ~n", [Hop, ToNode, From, Id, Via]),
                                                    case From==ToNode of
                                                        true ->
                                                            lager:error("From==ToNode so forwarding lower hopcount ~p ToNode ~p  From ~p for Id ~p Via ~p ~n", [Hop, ToNode, From, Id, Via]),
                                                            found_sub_aq_lockpath(Id, ToNode, Via, From, Hop);
                                                        false -> ok
                                                    end,
                                                    lager:error("Lower hopcount connection status ~p ~n", [get_connections()]),
                                                    lager:error("Lower hopcount members status ~p ~n", [get_peers()]);
                                                    %found_sub_aq_lockpath(Id, ToNode, Via, From, Hop);
                                                    %lockpath_unlock(Id),
                                                    %ets:delete(find_sub_aq, lists:nth(1,ets:match_object(find_sub_aq, {Id,'_', '_', '_' }))),
                                                    %found_sub_aq_lockpath(Id, ToNode, Via, From, Hop);
                                                false ->
                                                    lager:error("HopCount is more than existing, skipping.....")
                                            end;
                                        false -> 
                                            found_sub_aq_lockpath(Id, ToNode, Via, From, Hop)
                                    end;
                                false -> 
                                    case ets:member(peer_rates, Via) of
                                        true ->
                                            lager:error("LASPVIN ToNode ~p and Via are Peers.. Can send Acklock directly to ToNode (Id:~p , ToNode:~p, Via:~p, From:~p) ~n", [ToNode, Id, ToNode, Via, Via]),
                                            found_sub_aq_lockpath(Id, ToNode, Via, From, Hop),
                                            ok;
                                        false ->
                                            lager:error("Via not in peers, and ToNode is in peer, Not sending lock expecting ToNode to get back as to node is in peer_rates, connections:~p ~n", [get_connections()])
                                    end
                            end;
                        false ->
                            %Change this to if psudopeer exists, and add psudopeer in found_sub_aq_lockpath
                            case lists:member([ToNode], ets:match(find_sub_aq, {'_', '$1', '_', '_'})) of
                                true -> 
                                    lager:error("LASPVIN path ToNode: ~p exists ~p ~n",[ToNode, ets:match(find_sub_aq, {'_', '$1', '_', '_'})]),
                                    case Hop < lists:nth(1,lists:nth(1, ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))) of
                                        true ->
                                            lager:error("Got new path with lower hopcount ~p ToNode ~p  From ~p for Id ~p Via ~p ~n", [Hop, ToNode, From, Id, Via]),
                                            lager:error("Lower hopcount connection status ~p ~n", [get_connections()]),
                                            lager:error("Lower hopcount members status ~p ~n", [get_peers()]),
                                            found_sub_aq_lockpath(Id, ToNode, Via, From, Hop);
                                            %lockpath_unlock(Id),
                                            %ets:delete(find_sub_aq, lists:nth(1,ets:match_object(find_sub_aq, {Id,'_', '_', '_' }))),
                                            %found_sub_aq_lockpath(Id, ToNode, Via, From, Hop);
                                        false ->
                                            lager:error("HopCount is more than existing, skipping.....")
                                    end;
                                false -> found_sub_aq_lockpath(Id, ToNode, Via, From, Hop)
                            end
                            %case ets:member(c1, "pseudopeer") of
                            %    true -> 
                            %        %case lists:member(ToNode, ets:lookup_element(c1, "pseudopeer", 2)) of
                            %        case lists:member([ToNode], ets:match(find_sub_aq, {'_', '$1', '_'})) of
                            %            true -> lager:debug("LASPVIN path ToNode: ~p exists ~n",[ToNode]);
                            %            false -> found_sub_aq_lockpath(Id, ToNode, From)
                            %        end;
                            %    false -> found_sub_aq_lockpath(Id, ToNode, From)
                            %end
                    end;
                false ->
                    found_sub_aq_lockpath(Id, ToNode, Via, From, Hop)
            end
    end,
    {noreply, State};

handle_cast({find_sub_aq_lock, Id, ToNode, From}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub_aq_lock"),
    lager:debug("LASPVIN Store ~p ~n",[Store]),
    lager:error("LASPVIN received find_sub_aq_lock for Id:~p ToNode~p From:~p ~n", [Id, ToNode, From]),
    case ets:lookup_element(peer_rates, "self_rate", 2)==lists:nth(1,lists:nth(1,ets:match(find_sub, {'$1',Id, '_', '_' }))) of
        true ->
            lager:error("LASPVIN Rate updated already ~n"),
            forward_aq_lock(Id, ToNode, From);
        false ->
            lager:error("LASPVIN updating rate ~n"),
            ets:update_element(peer_rates, "self_rate", {2, lists:nth(1, lists:nth(1,ets:match(find_sub, {'$1', Id, '_', '_'})))}),
            %Update scubscription if not already subscribed to c1
            %ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]),
            %?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
            %propagate update_rate for all? may be at the end of the function,
            forward_aq_lock(Id, ToNode, From),
            timer:sleep(5),
            lager:error("Deleting this rate_ack after sending updated rate ~p ~n", [ets:tab2list(rate_ack)]),
            lager:error("Rate_ack shown and this are the connections ~p ~n", [get_connections()]),
            lists:foreach(fun(PeerT) ->
                {Peer} = PeerT,
                lager:error("Sending updated rate ~p to ~p ~n", [ets:lookup_element(peer_rates, "self_rate", 2), Peer]),
                ?SYNC_BACKEND:send(?MODULE, {rate_class, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, Peer),
                ets:delete(rate_ack, Peer)
                end, ets:tab2list(rate_ack))
    end,
    %ets:delete_object(find_sub, lists:nth(1,ets:match_object(find_sub, {'_', Id, '_'}))),
    {noreply, State};

handle_cast({find_sub_aq_lock_rev, Id, From}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub_aq_lock_rev"),
    lager:debug("LASPVIN Store ~p ~n",[Store]),
    lager:error("LASPVIN received find_sub_aq_lock_rev for Id:~p From:~p ~n", [Id, From]),
    case ets:lookup_element(peer_rates, "self_rate", 2)==lists:nth(1,lists:nth(1,ets:match(find_sub, {'$1',Id, '_', '_' }))) of
        true ->
            lager:error("LASPVIN Rate updated already ~n"),
            forward_aq_lock_rev(Id);
        false ->
            lager:error("LASPVIN updating rate ~n"),
            ets:update_element(peer_rates, "self_rate", {2, lists:nth(1, lists:nth(1,ets:match(find_sub, {'$1', Id, '_', '_'})))}),
            %Update scubscription if not already subscribed to c1
            %ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]),
            %?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)))
            %propagate update_rate for all? may be at the end of the function,
            forward_aq_lock_rev(Id),
            ets:delete_all_objects(rate_ack)
    end,
    %ets:delete_object(find_sub, lists:nth(1,ets:match_object(find_sub, {'_', Id, '_'}))),
    {noreply, State};

handle_cast({find_sub, From, ReqRate, Id, Hop}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("find_sub"),
    lager:debug("LASPVIN store:~p ~n", [Store]),
    lager:error("LASPVIN received find_sub Id: ~p From: ~p Hop:~p ~n", [Id, From, Hop]),
    lager:error("At receiving Id ~p find_sub: ~p ~n", [Id, ets:tab2list(find_sub)]),
    case ets:member(find_sub_aq, Id) of
        true ->
            lager:error("LASPVIN find_sub_aq exists for this find_sub ~p ~n", [Id]),
            ok;
        false ->
            timer:sleep(2),
            case ets:lookup_element(peer_rates, "self_rate", 2) == ReqRate of
                true ->
                    timer:sleep(2),
                    insert_findSub(ReqRate, Id, From, Hop),
                    %ets:insert(c1, [{"pseudopeer", erlang:list_to_atom(string:sub_string(Id, 1, string:len(Id)-2)), Hop}]),
                    found_sub(Id, lasp_support:mynode(), lasp_support:mynode(), 0);
                false -> check_sub_exists(From, ReqRate, Id, Hop)
            end,
            case ets:member(find_sub_aq, Id) of
                true ->
                    case ReqRate of
                        "c1" -> forward_sub_req(Id, Hop);
                        "c2" -> lager:debug("LASPVIN Skip forwarding for class c2")
                    end;
                false -> lager:error("LASPVIN Request forwarded~n")
            end
    end,
    {noreply, State};

handle_cast({rate_ack, From, Rate}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_ack"),
    lager:debug("LASPVIN received ack from ~p Store ~p ~n", [From, Store]),
    case Rate ==  ets:lookup_element(peer_rates, "self_rate", 2) of
       true -> ets:insert(rate_ack, [{From}]), ets:insert(myconnections, [{"connect",From}]);
       false -> ok
    end,
    {noreply, State};

handle_cast({check_tonode, ToNode, Hop, From}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_ack"),
    lager:debug("LASPVIN received ack from ~p Store ~p ~n", [From, Store]),
    lager:error("Received check_tonode From:~p for ToNode:~p Hop:~p ~n", [From, ToNode, Hop]),
    %TO BE DONE!!!!
    {noreply, State};

handle_cast({sub_cancel, From, Id, ToNode}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("rate_ack"),
    lager:debug("LASPVIN received node_cancel from ~p Store ~p ~n", [From, Store]),
    lager:error("Received sub_cancel From:~p for Id:~p ~n", [From, Id]),
    case ets:member(c1, "subscriber") of
        true ->
            case lists:member(From, ets:lookup_element(c1, "subscriber", 2)) of
                true-> 
                    case length(ets:lookup_element(c1, "subscriber", 2)) > 3 of
                        true ->
                            lager:error("sub_cancel but more than 3 c1 subscrier ~p ~n", ets:tab2list(c1));
                        false ->
                            ets:update_element(peer_rates, "self_rate", {2, os:getenv("RATE_CLASS", "c1")}),
                            lager:error("Resetted self_rate ~p and deleting rate_ack ~n", [ets:tab2list(peer_rates)]),
                            ets:delete_all_objects(rate_ack),
                            case ets:match(find_sub_aq, {Id, ToNode, '$1','_'}) == [[lasp_support:mynode()]] of
                                    true -> 
                                        lager:error("End of sub_cancel chain ~p");
                                    false ->
                                        [[ForwardCancelTo]] = ets:match(find_sub_aq, {Id, ToNode, '$1','_'}),
                                        ?SYNC_BACKEND:send(?MODULE, {sub_cancel, lasp_support:mynode(), Id, ToNode}, ForwardCancelTo)
                            end
                    end;
                false -> lager:error("Sub_cancel From ~p is not a c1 subscriber ~p ~n", [From, ets:tab2list(c1)])
            end;
        false -> lager:error("sub_cancel but no c1 subscriber ~p ~n", [ets:tab2list(c1)])
    end,
    %TO BE DONE!!!!
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
                      found_sub(Id, From, lasp_support:mynode(), 1)
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
                           false -> 
                               lager:error("Sending rate ~p to ~p ~n", [ets:lookup_element(peer_rates, "self_rate", 2), Peer]),
                               ?SYNC_BACKEND:send(?MODULE, {rate_class, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, Peer)
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
        true -> 
            propagate_by_class(c1, "peer");
            %propagate_by_class(c1, "subscriber");
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
insert_findSub(ReqRate, Id, From, Hop)->
    timer:sleep(10),
    case lists:member(erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)), get_connections()) of
                        true -> 
                            lager:error("Request Id ~p is from a Peer directly connected", [Id]),
                            ets:insert(find_sub, {ReqRate, Id, erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)), 1}),
                            lager:error("find_sub with Id in get_connections(): ~p ~n", [ets:tab2list(find_sub)]);
                        false -> 
                            lager:error("Id is not get_connections(): ~p ~n", [get_connections()]),
                            ets:insert(find_sub, {ReqRate, Id, From, Hop}),
                            lager:error("find_sub after Id not get_connections(): ~p ~n", [ets:tab2list(find_sub)])
    end.

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
    lager:error("Received new rate from ~p with new rate ~p", [From, NewRate]),
    case OldRate of
        "c1" -> ets:delete_object(c1, {"peer", From});
        "c2" -> ets:delete_object(c2, {"peer", From});
        "c3" -> ets:delete_object(c3, {"peer", From})
    end,
    peer_rate_insert(From, NewRate),
    ets:update_element(peer_rates, From, {2, NewRate}).
    %timer:sleep(5500),
    %lager:error("Checking sub_cancel"),
    %case ets:lookup_element(peer_rates, "self_rate", 2) == "c1" of
    %    true ->
    %       case ets:member(peer_rates, "subscription") of
    %            true ->
    %                case lists:member([ets:lookup_element(peer_rates, "subscription", 2)], ets:match(find_sub_aq, {'_', '_', '$1', '_'})) of
    %                            true ->
    %                                case NewRate == "c1" of
    %                                    true ->
    %                                        lager:error("Sending sub_cancel to ~p ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
    %                                        IdCancel = 
    %                                        ?SYNC_BACKEND:send(?MODULE, {sub_cancel, lasp_support:mynode(), lists:flatten(ets:match(find_sub_aq, {'$1', '_', ets:lookup_element(peer_rates, "subscription", 2), '_'}))}, ets:lookup_element(peer_rates, "subscription", 2)),
    %                                        ets:delete(peer_rates, "subscription"),
    %                                        ets:insert(peer_rates, [{"subscription", From}]),
    %                                        ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, From),
    %                                        lager:error("Subscribed to ~p  and sending sub_cancel to ~p ~n", [From]);
    %                                   false ->
    %                                       ok
    %                               end;
    %                           false ->
    %                               ok
    %                       end
    %               end;
    %           false ->
    %               ok
    %       end;
    %   false ->
    %       ok
    %end.

%% @private
schedule_delta_garbage_collection() ->
    timer:send_after(?DELTA_GC_INTERVAL, delta_gc).

%% @private
schedule_rate_class_info_propagation() ->
    lager:debug("LASPVIN test"),
    timer:send_after(5000, rate_info).

%% @private
schedule_rate_propagation_c1() ->
    lager:debug("LASPVIN rate_propagation_c1"),
    %lager:error("C1 propagation ~p ~n", [time_stamp()]),
    %5000 milliseconds is 5 seconds
    timer:send_after(5000, rate_prop_c1).

%% @private
schedule_rate_propagation_c2() ->
    lager:debug("LASPVIN rate_propagation_c2"),
    %lager:error("C2 propagation ~p ~n", [time_stamp()]),
    %22500 milliseconds is 22.5 seconds
    timer:send_after(22500, rate_prop_c2).

%% @private
schedule_rate_propagation_c3() ->
    lager:debug("LASPVIN rate_propagation_c3"),
    %lager:error("C3 propagation ~p ~n", [time_stamp()]),
    %22500 milliseconds is 22.5 seconds
    timer:send_after(45500, rate_prop_c3).

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
get_connections() ->
    case ets:first(myconnections) of
        '$end_of_table' ->
            get_peers();
        _Else ->
            case lists:flatlength(get_peers()) > lists:flatlength(ets:lookup_element(myconnections, "connect", 2)) of
                true -> get_peers();
                false -> ets:lookup_element(myconnections, "connect", 2)
            end
    end.

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
                         true -> 
                            lager:error("Subscribing to ~p case1 ~n", lists:nth(1, ets:lookup_element(c1, "peer", 2))),
                            ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]), 
                            ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                         false -> 
                            lager:debug("LASPVIN no peer to subscribe Case 1 ~n ")
                      end;
                   false ->
                      case ets:member(c2, "peer") of
                         true ->  
                            lager:error("Subscribing to ~p case2 ~n", lists:nth(1, ets:lookup_element(c2, "peer", 2))),
                            ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c2, "peer", 2))}]), 
                            ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c2, "peer", 2)));
                         false ->
                            case ets:member(c1, "peer") of
                               true ->
                                   lager:error("Subscribing to ~p case3 ~n", lists:nth(1, ets:lookup_element(c1, "peer", 2))),
                                   ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]),
                                   ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                               false -> lager:debug("LASPVIN no peer to subscribe case 2 ~n")
                            end
                      end
                end;
             false ->
                case ets:member(c1, "peer") of
                   true -> 
                       lager:error("Subscribing to ~p case4 ~n", lists:nth(1, ets:lookup_element(c1, "peer", 2))),
                       ets:insert(peer_rates, [{"subscription", lists:nth(1, ets:lookup_element(c1, "peer", 2))}]),
                       ?SYNC_BACKEND:send(?MODULE, {rate_subscribe, lasp_support:mynode(), ets:lookup_element(peer_rates, "self_rate", 2)}, lists:nth(1, ets:lookup_element(c1, "peer", 2)));
                   false ->
                      case ets:member(find_sub, ets:lookup_element(peer_rates, "self_rate", 2)) of
                         true -> lager:debug("Find_sub_req exists for the class");
                         false -> 
                            insert_findSub("c1", erlang:atom_to_list(lasp_support:mynode())++"c1", lasp_support:mynode(), 0),
                            lager:error("Requesting c1 sub ~n"),
                            forward_sub_req(erlang:atom_to_list(lasp_support:mynode())++"c1", 0)
                      end
                end
          end
    end.

%%private
forward_sub_req(Id, Hop) ->
   lager:debug("LASPVIN no c1 peer to subscribe forwarding to peers ~n"),
   lager:error("Forwarding Req Id ~p to Peers RcvHop:~p IncHop:~p ~n", [Id, Hop, Hop+1]),
   timer:sleep(2),
   NewHop = Hop+1,
   lists:foreach(fun(Peer) ->
      case lists:member(Peer, ets:lookup_element(find_sub, "c1", 3)) of
         true -> ok;
         false ->
             case erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)) == Peer of
               true -> lager:debug("LASPVIN Peer ~p is Source of Req.. Skipping ~n",[Peer]);
               false ->
                   case lists:member([Peer], lists:nth(1, ets:match(find_sub, {'_', Id, '$1', '_'}))) of
                       true -> ok;
                       false ->
                           lager:error("Forwarding ReqId ~p to Peer:~p Hop:~p ~n", [Id, Peer, NewHop]),
                           ?SYNC_BACKEND:send(?MODULE, {find_sub, lasp_support:mynode(),"c1", Id, NewHop}, Peer)
                    end
            end
      end
   end,
   get_connections()).

%% @private
check_sub_exists(From, ReqRate, Id, Hop) ->
    lager:error("Checking sub_exists for Id:~p From:~p Hop:~p ~n", [Id, From, Hop]),
    timer:sleep(2),
    case ets:member(find_sub, ReqRate) of
       true ->
          case lists:member(Id, ets:lookup_element(find_sub, ReqRate, 2)) of
             true -> 
                 lager:debug("LASPVIN Find_sub request id ~p exists ~n", [Id]),
                 case Hop < lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '_', '$1'}))) of
                    true -> 
                        lager:error("Find_sub request id ~p exists but Rcv hopcount ~p is lower than existing ~p ~n", [Id, Hop, lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '_', '$1'})))]),
                        ets:delete_object(find_sub, {'_', Id, '_', '_'}),
                        insert_findSub(ReqRate, Id, From, Hop),
                        lager:error("Deleted rqeuest id, updated it with lower hopcount ~p ~n", [ets:tab2list(find_sub)]);
                    false -> ok
                end;
             false -> 
                lager:error("LASPVINDEBUG Matching find_sub rates found for Id: ~p but forwarding  ~n", [Id]),
                insert_findSub(ReqRate, Id, From, Hop),
                forward_sub_req(Id, Hop)
                %lager:debug("LASPVIN find_sub:insert ReqRate:~p Id:~p From:~p ~n", [ReqRate, Id, From]),
                %%%ERROR HERE
                %lager:error("LASPVINDEBUG Informing ~p that found peer for ID:~ toNode: ~p Via: ~p HopCount: ~p +1 ~n", [lists:nth(1,ets:lookup_element(find_sub, ReqRate, 3)), lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)), string:substr(Id, 1, string:len(Id)-2), From, Hop]),
                %To be implemented for reverse found_sub(lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)), erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)), From, Hop+1),
                %ets:insert(match_sub_aq, [{Id, lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2))}]),
                %ets:insert(match_sub_aq, [{lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)), Id}]),
                %insert_findSub(ReqRate, Id, From, Hop),
                %lager:error("LASPVINDEBUG Added match_sub_aq ~p ~n", [ets:tab2list(match_sub_aq)]),
                %lager:error("LASPVIN Informing ~p that found peer for ID:~ toNode: ~p Via:~p HopCount:~p+1 ~n", [From, Id, erlang:list_to_atom(string:sub_string(lists:nth(1,ets:lookup_element(find_sub, ReqRate, 2)), 1, string:len(lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)))-2)), lists:nth(1,ets:lookup_element(find_sub, "c1", 3))]),
                %found_sub(Id, erlang:list_to_atom(string:sub_string(lists:nth(1,ets:lookup_element(find_sub, ReqRate, 2)), 1, string:len(lists:nth(1, ets:lookup_element(find_sub, ReqRate, 2)))-2)), lists:nth(1,ets:lookup_element(find_sub, "c1", 3)), lists:nth(1, ets:lookup_element(find_sub, "c1", 4))+1)
          end;
       false ->
          insert_findSub(ReqRate, Id, From, Hop),
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
                                       true -> found_sub(Id, lists:nth(2, ets:lookup_element(c1, "peer", 2)), lasp_support:mynode(), 1);
                                       false -> found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)), lasp_support:mynode(), 1)
                                    end;
                               false -> lager:debug("LASPVIN forward request to peers"), forward_sub_req(Id, Hop)
                            end;
                         false ->
                             found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)), lasp_support:mynode(), 1)
                      end;
                   false -> lager:debug("LASPVIN send to peers"), forward_sub_req(Id, Hop)
                end;
             "c2" ->
                case ets:member(c2, "peer") of
                   true ->
                      case lists:member(From, ets:lookup_element(c2, "peer", 2)) of
                         true -> 
                            case length(ets:lookup_element(c2, "peer", 2)) > 1 of
                               true ->
                                   case lists:nth(1, ets:lookup_element(c2, "peer", 2)) == From of
                                       true -> found_sub(Id, lists:nth(2, ets:lookup_element(c2, "peer", 2)), lasp_support:mynode(), 1);
                                       false -> found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)), lasp_support:mynode(), 1)
                                    end;
                               false -> lager:debug("LASPVIN forward request to peers ~n"), forward_sub_req(Id, Hop)
                            end;
                         false ->
                             found_sub(Id, lists:nth(1, ets:lookup_element(c2, "peer", 2)), lasp_support:mynode(), 1)
                      end;
                   false -> 
                      case ets:member(c1, "peer") of
                         true -> 
                             found_sub(Id, lists:nth(1, ets:lookup_element(c1, "peer", 2)), lasp_support:mynode(), 1);
                         false -> lager:debug("LASPVIN send to peers"), forward_sub_req(Id, Hop)
                      end
                end
          end
    end.

%%private
forward_find_sub_on_join(From) ->
    lager:error("Forwarding find_sub:~p to ~p on join ~n", [ets:tab2list(find_sub), From]),
    lists:foreach(fun(ReqRate) ->
                          case From == lists:nth(1,ets:lookup_element(find_sub, lists:nth(1,ReqRate), 3)) of
                             true -> ok;
                             false ->lager:error("LASPVIN sent find_sub on join req Id:~p, Hop:~p ~n", [ lists:nth(1,ets:lookup_element(find_sub, lists:nth(1,ReqRate), 2)), (lists:nth(1,ets:lookup_element(find_sub, lists:nth(1, ReqRate), 4))+1)]), ?SYNC_BACKEND:send(?MODULE, {find_sub, lasp_support:mynode(), lists:nth(1,ReqRate), lists:nth(1,ets:lookup_element(find_sub, lists:nth(1,ReqRate), 2)), (lists:nth(1,ets:lookup_element(find_sub, lists:nth(1, ReqRate), 4))+1)}, From)
                          end
                        end,
                  lists:usort(ets:match(find_sub, {'$1', '_', '_', '_'}))).

%%private
found_sub(Id, ToNode, Via, Hop) ->

    case erlang:list_to_atom(string:substr(Id, 1, string:len(Id)-2)) == ToNode of
        true -> lager:debug("LASPVIN False call");
        false ->
            lager:error("LASPVIN found the peer at ~p for ID: ~p ToNode: ~p Via:~p ~n", [time_stamp(), Id, ToNode, Via]),
            case ets:member(find_sub_aq, Id) of
                true -> 
                    lager:error("LASPVIN find_sub_aq Id exists not forwarding found_sub"),
                    case Hop < lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '_', '$1'}))) of
                        true -> 
                            lager:error("Sub_aq exists for Id:~p but HopCount:~p is lower than existing hopcount:~p .. Forwarding request ~n", [Id, Hop, lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '_', '$1'})))]),
                            ets:insert(find_sub_aq, [{Id, ToNode, lasp_support:mynode(), Hop}]),
                            ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, Via, lasp_support:mynode(), Hop}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))));
                        false ->
                            lager:error("Sub_aq exists and Hop:~p > existing HopCount:~p ~n", [Hop, lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '_', '$1'})))])
                    end,
                    ok;
                false ->
                    %timer:sleep(5),
                    case Via == lasp_support:mynode() of
                        true -> 
                            found_sub_aq_lockpath(Id, ToNode, Via, lasp_support:mynode(), 0);
                        false -> 
                            ets:insert(find_sub_aq, [{Id, ToNode, lasp_support:mynode(), Hop}]),
                            ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, Via, lasp_support:mynode(), Hop}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))))
                    end
            end
    end.


%%private
found_sub_aq_lockpath(Id, ToNode, Via, From, Hop) ->
    lager:error("LASPVINDEBUGERROR find_sub ~p ~n",[ets:tab2list(find_sub)] ),
    timer:sleep(2),
    lager:error("LASPVINERROR here ~p ~n", [ets:match(find_sub, {'_', Id, '$1', '_'})]),
    case ets:member(find_sub, "c1") of
        true ->
            case lists:member(Id, ets:lookup_element(find_sub, "c1", 2)) of
                true ->
                    case lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))) == lasp_support:mynode() of
                        true ->
                            case ets:first(find_sub_aq) of
                                '$end_of_table' -> ok;
                                _Else ->
                                    timer:sleep(10000)
                            end,
                            case lists:member(ToNode, get_connections()) of
                                true ->
                                    case Via == lasp_support:mynode() of
                                        true -> 
                                             lager:error("Find_sub_aq ToNodes before checking if ToNode ~p exists : ~p", [ToNode, ets:match(find_sub_aq, {'_', '$1', '_', '_'})]),
                                             case lists:member([ToNode], ets:match(find_sub_aq, {'_', '$1', '_', '_'})) of
                                                true ->
                                                    case Hop < lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))) of
                                                        true ->
                                                            case ets:member(peer_rates, "subscription") of
                                                                true -> 
                                                                    lager:error("Got lower path canceChecking if subscription ~p has path ToNode ~p as got lower hop count and potential lock ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
                                                                    ets:insert(temp_tonode, [{Id, ToNode, Via, Hop}]),
                                                                    ?SYNC_BACKEND:send(?MODULE, {check_tonode, ToNode, Hop, lasp_support:mynode()}, ets:lookup_element(peer_rates, "subscription", 2));
                                                                false ->
                                                                    lager:error("No subscription yet and got lower hopcount ~p and potential lock ~n", [ets:tab2list(peer_rates)]),
                                                                    lager:error("ToNode ~p is not a pseudopeer ~p ~n", [ToNode, ets:tab2list(c1)]),
                                                                    send_lock(Id, ToNode, Via, Hop, From)
                                                            end;
                                                        false ->
                                                            lager:error("LASPVIN path ToNode: ~p exists and Hop ~p > existing Hop ~p in find_sub_aq: ~p ~n",[ToNode, Hop, lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))), ets:match_object(find_sub_aq, {'_', '$1', '_', '_'})])
                                                    end;
                                                false ->
                                                    case ets:member(c1, "pseudopeer") of
                                                        true ->
                                                            case lists:member(ToNode, ets:lookup_element(c1, "pseudopeer", 2)) of
                                                                true -> 
                                                                    lager:error("ToNode ~p exists in c1 pseudopeer ~p ~n", [ToNode, ets:lookup_element(c1, "pseudopeer", 2)]);
                                                                false ->
                                                                    timer:sleep(2),
                                                                    case ets:member(peer_rates, "subscription") of
                                                                        true -> 
                                                                            lager:error("Checking if subscription ~p has path ToNode ~p ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
                                                                            ets:insert(temp_tonode, [{Id, ToNode, Via, Hop}]),
                                                                            ?SYNC_BACKEND:send(?MODULE, {check_tonode, ToNode, Hop, lasp_support:mynode()}, ets:lookup_element(peer_rates, "subscription", 2));
                                                                        false ->
                                                                            lager:error("No subscription yet ~p ~n", [ets:tab2list(peer_rates)]),
                                                                            lager:error("ToNode ~p is not a pseudopeer ~p ~n", [ToNode, ets:tab2list(c1)]),
                                                                            lager:error("Path ToNode:~p does not exists in find_sub_aq:~p", [ToNode, ets:tab2list(find_sub_aq)]),
                                                                            send_lock(Id, ToNode, Via, Hop, From)
                                                                    end
                                                            end;
                                                        false ->
                                                            %Check with subscription if it has path to this ToNode,
                                                            timer:sleep(2),
                                                            case ets:member(peer_rates, "subscription") of
                                                                true ->
                                                                    lager:error("Checking if subscription ~p has path ToNode ~p ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
                                                                    ets:insert(temp_tonode, [{Id, ToNode, Via, Hop}]),
                                                                    ?SYNC_BACKEND:send(?MODULE, {check_tonode, ToNode, Hop, lasp_support:mynode()}, ets:lookup_element(peer_rates, "subscription", 2));
                                                                false ->
                                                                    lager:error("Path ToNode:~p does not exists in find_sub_aq:~p", [ToNode, ets:tab2list(find_sub_aq)]),
                                                                    send_lock(Id, ToNode, Via, Hop, From)
                                                            end
                                                    end
                                             end;
                                        false -> 
                                            lager:error("ToNode ~p is in connections ~p ~n", [ToNode, get_connections()]),
                                            lager:error("Subscription must be done already ToNode ~p ~n", [ets:tab2list(c1)])
                                    end;
                                false ->
                                    case Via==From of
                                        true ->
                                             further_checks(Id, ToNode, Via, From, Hop);
                                        false ->
                                            case lists:member(Via, get_connections()) of
                                                true -> lager:error("Via is in connections ~p ... Skipping...", [get_connections()]);
                                                false -> further_checks(Id, ToNode, Via, From, Hop)
                                            end
                                    end
                            end;
                        false ->
                            case Via == lasp_support:mynode() of
                                true ->
                                    case From == lasp_support:mynode() of
                                        true ->
                                            ets:insert(find_sub_aq, [{Id, ToNode, From, Hop}]),
                                            lager:error("LASPVINDEBUG Forwarding find_sub_aq for Id: ~p ToNode:~p From:~p to ~p HopCount:~p ~n", [Id, ToNode, From, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))), Hop+1]), 
                                            ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, From, lasp_support:mynode(), Hop+1}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))));
                                        false ->
                                            lager:error("Via is from self... Skipping Id:~p From:~p Hop:~p", [Id, From, Hop])
                                    end;
                                false ->
                                    case lists:member(ToNode, get_connections()) of
                                        true -> lager:error("ToNode ~p is a peer ~p.. Not forwarding req...", [ToNode, ets:tab2list(peer_rates)]);
                                        false ->
                                            ets:insert(find_sub_aq, [{Id, ToNode, From, Hop}]),
                                            lager:error("LASPVINDEBUG Forwarding find_sub_aq for Id: ~p ToNode:~p From:~p to ~p HopCount:~p ~n", [Id, ToNode, From, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))), Hop+1]), 
                                            ?SYNC_BACKEND:send(?MODULE, {find_sub_aq, Id, ToNode, From, lasp_support:mynode(), Hop+1}, lists:nth(1, lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))))
                                    end
                            end
                    end;
                false ->
                    lager:error("LASPVIN ID ~p not in find_sub ~p ~n", [Id, ets:tab2list(find_sub)])
            end;
        false ->
            lager:error("Id ~p not yet in find_sub: ~p ~n", [Id, ets:tab2list(find_sub)])
    end.



%%private
forward_aq_lock(Id, ToNode, From) ->
    case lists:nth(1, ets:lookup_element(find_sub_aq, Id, 3)) == lasp_support:mynode() of
                true ->
                    case ets:member(match_sub_aq, Id) of
                        true ->
                            lager:error("LASPVIN forwarding Matching Request Lock for Id ~p From:~p ~n", [Id, From]),
                            %Gandtay
                            case lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))) == From of
                                true -> 
                                    lager:error("LASPVIN matching 1 Have to Send lock_rev for Id:~p  Find_sub: ~p Match_sub_aq: ~p ~n", [Id, ets:tab2list(find_sub), ets:tab2list(match_sub_aq)]),
                                    ok;
                                    %?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock_rev, ets:lookup_element(match_sub_aq, Id, 2), lasp_support:mynode()},lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', ets:lookup_element(match_sub_aq, Id, 2), '$1'}))));                                
                                false ->
                                    lager:error("LASPVIN matching 2 Have to Send lock_rev for Id:~p  Find_sub: ~p Match_sub_aq: ~p ~n", [Id, ets:tab2list(find_sub), ets:tab2list(match_sub_aq)]),
                                    %lager:error("LASPVIN sending lock_rev to ~p ~n", [lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1'})))]),
                                    ok
                                    %?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock_rev, ets:lookup_element(match_sub_aq, Id, 2), lasp_support:mynode()},lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1'}))))
                            end;
                        false ->
                            %check find_sub if there are any other nodes requiring same rate,
                            %if there are inform them 
                            lager:error("LASPVIN Locking reached chain end for Id:~p Reveied from ~p ~n", [Id, From])
                    end;
                false ->
                    %pass on the lock & delete find_sub_aq entry
                    lager:error("LASPVIN Forwarding lock for ID:~p to ~p ~n", [Id, ets:lookup_element(find_sub_aq, Id, 3)]),
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock, Id, ToNode, lasp_support:mynode()}, lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {Id, ToNode, '$1', '_'}))))
                    %ets:delete(find_sub_aq, Id)
    end.

%%private
further_checks(Id, ToNode, Via, From, Hop) ->
    lager:error("Find_sub_aq ToNodes before checking if ToNode ~p exists : ~p", [ToNode, ets:match(find_sub_aq, {'_', '$1', '_', '_'})]),
    case lists:member([ToNode], ets:match(find_sub_aq, {'_', '$1', '_', '_'})) of
        true ->
            lager:error("LASPVIN path ToNode: ~p exists in find_sub_aq: ~p ~n",[ToNode, ets:match_object(find_sub_aq, {'_', '$1', '_', '_'})]),
            case Hop < lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))) of
                true ->
                    lager:error("Got lower hop checking subscription: ~p", ets:tab2list(peer_rates)),
                    timer:sleep(25000),
                    case ets:member(peer_rates, "subscription") of
                        true ->
                            lager:error("Lower hop subscription exists"),
                            case lists:member([ets:lookup_element(peer_rates, "subscription", 2)], ets:match(find_sub_aq, {'_', ToNode, '$1', '_'})) of
                                true ->
                                    % Cancel subscription and send lock for new,
                                    case From == ets:lookup_element(peer_rates, "subscription", 2) of
                                        true -> lager:error("Not sending cancel as From==Subscription ~n");
                                        false ->
                                            lager:error("Lower hop Subscription is locked for this ToNode ~p sending cancel ~n", [ToNode]),
                                            ?SYNC_BACKEND:send(?MODULE, {sub_cancel, lasp_support:mynode(), Id, ToNode}, ets:lookup_element(peer_rates, "subscription", 2)),
                                            lager:error("Lower hop Deleted Subscription"),
                                            ets:delete(peer_rates, "subscription")
                                    end,
                                    case ets:member(find_sub_aq, Id) of
                                        true ->
                                            lager:error("Deleting existing find_sub_aq ~n"),
                                            lists:foreach(fun(Sub_aq) -> 
                                                ets:delete_object(find_sub_aq, Sub_aq)
                                            end, ets:match_object(find_sub_aq, {Id, ToNode, '_', '_'}));
                                            %[DeleteAq] = ets:match_object(find_sub_aq, {Id, ToNode, '_', '_'}),
                                            %ets:delete_object(find_sub_aq, DeleteAq);
                                        false ->
                                            ok
                                    end,
                                    send_lock(Id, ToNode, Via, Hop, From);
                                false ->
                                    send_lock(Id, ToNode, Via, Hop, From)
                            end;
                        false -> 
                            lager:error("Susbcription not found ~p sending lock lower hop ~n", ets:tab2list(peer_rates)),
                            send_lock(Id, ToNode, Via, Hop, From)
                    end;
                false -> lager:error("RcvHop ~p > Existing Hop ~p for ToNode~p ~n", [Hop, lists:nth(1,lists:nth(1,ets:match(find_sub_aq, {'_', ToNode, '_', '$1'}))), ToNode])
            end;
        false ->
            case ets:member(c1, "pseudopeer") of
                true ->
                    case lists:member(ToNode, ets:lookup_element(c1, "pseudopeer", 2)) of
                        true -> 
                            lager:error("ToNode ~p exists in c1 pseudopeer ~p ~n", [ToNode, ets:lookup_element(c1, "pseudopeer", 2)]),
                            case Hop < lists:nth(1,lists:nth(1,ets:match(c1, {'_', ToNode, '$1'}))) of
                                true ->
                                    [PseudoDelete] = ets:match_object(c1, {'_', ToNode, '$1'}),
                                    ets:delete_object(c1, PseudoDelete),
                                    send_lock(Id, ToNode, Via, Hop, From);
                                false ->
                                    lager:error("No adding as RCV HopCount ~p is > Existing Hopcount ~p ~n", [Hop, lists:nth(1,lists:nth(1,ets:match(c1, {'_', ToNode, '$1'})))])
                            end;
                        false ->
                            timer:sleep(2),
                            case ets:member(peer_rates, "subscription") of
                                true -> 
                                    lager:error("Checking if subscription ~p has path ToNode ~p ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
                                    ets:insert(temp_tonode, [{Id, ToNode, Via, Hop}]),
                                    ?SYNC_BACKEND:send(?MODULE, {check_tonode, ToNode, Hop, lasp_support:mynode()}, ets:lookup_element(peer_rates, "subscription", 2));
                                false ->
                                    lager:error("No subscription yet ~p ~n", [ets:tab2list(peer_rates)]),
                                    lager:error("ToNode ~p is not a pseudopeer ~p ~n", [ToNode, ets:tab2list(c1)]),
                                    lager:error("Path ToNode:~p does not exists in find_sub_aq:~p", [ToNode, ets:tab2list(find_sub_aq)]),
                                    send_lock(Id, ToNode, Via, Hop, From)
                            end
                    end;
                false ->
                    lager:error("Path ToNode:~p does not exists in find_sub_aq:~p", [ToNode, ets:tab2list(find_sub_aq)]),
                    timer:sleep(2),
                    case ets:member(peer_rates, "subscription") of
                               true -> 
                                    lager:error("Checking if subscription ~p has path ToNode ~p ~n", [ets:lookup_element(peer_rates, "subscription", 2)]),
                                    ets:insert(temp_tonode, [{Id, ToNode, Via, Hop}]),
                                    ?SYNC_BACKEND:send(?MODULE, {check_tonode, ToNode, Hop, lasp_support:mynode()}, ets:lookup_element(peer_rates, "subscription", 2));
                                false ->
                                    lager:error("No subscription yet ~p ~n", [ets:tab2list(peer_rates)]),
                                    send_lock(Id, ToNode, Via, Hop, From)
                    end
            end
    end.



%%private
forward_aq_lock_rev(Id) ->
    case lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))) == lasp_support:mynode() of
                true ->
                    lager:error("LASPVIN Chain end reached for reverse Id ~p ~n", [Id]);
                false ->
                    %pass on the lock & delete find_sub_aq entry
                    lager:error("LASPVIN Forwarding lock rev for ID:~p to ~p ~n", [Id, lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'})))]),
                    ?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock_rev, Id, lasp_support:mynode()},lists:nth(1,lists:nth(1,ets:match(find_sub, {'_', Id, '$1', '_'}))))
                    %ets:delete(find_sub_aq, Id)
    end.

%%private
send_lock(Id, ToNode, Via, Hop, From) ->
    case Hop == 0 of
        true ->
            lager:error("ToNode must be a peer ~p as From is self for Id ~p ~n", [ToNode, Id]),
            ets:insert(find_sub_aq, [{Id, ToNode, From, Hop}]),
            check_subscription();
        false ->
            lager:error("Sending aq_lock to From ~p For Via ~p for Id:~p ToNode:~p HopCount ~p and I am the source ~n", [From, Via, Id, ToNode, Hop]),
            ets:insert(find_sub_aq, [{Id, ToNode, From, Hop}]),
            lager:error("LASPVIN Got path to ~p ID:~p From:~p Via:~p HopCount:~p ~n", [ToNode, Id, Via, Via, Hop]),
            lager:error("LASPVIN Check if Via ~p in peer_rates: ~p", [Via, ets:tab2list(peer_rates)]),
            lager:error("LASPVIN Connections: ~p", [get_connections()]),
            ets:insert(c1, [{"pseudopeer", ToNode, Hop}]),
            lager:error("Sending Lock for Id ~p, ToNode:~p to ~p ~n", [Id, ToNode, From]),
            ?SYNC_BACKEND:send(?MODULE, {find_sub_aq_lock, Id, ToNode, lasp_support:mynode()}, From)
    end.



%lockpath_unlock(Id) ->
%    ets:insert(find_sub_aq, [{Id, ToNode, From, Hop}]),


%    .

%% @private
lists_min([]) -> 0;
lists_min(L) -> lists:min(L).
