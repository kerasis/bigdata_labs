from typing import Callable, Dict, List


VertexId = int
Message = float
Graph = Dict[VertexId, List[VertexId]]


def pregel(
    graph: Graph,
    initial_state: Dict[VertexId, float],
    superstep_fn: Callable[[VertexId, float, List[Message], List[VertexId]], float],
    num_iters: int,
) -> Dict[VertexId, float]:
    state = initial_state.copy()

    for _ in range(num_iters):
        # собираем сообщения
        inbox: Dict[VertexId, List[Message]] = {v: [] for v in graph.keys()}
        # отправка сообщений
        for v, neighbors in graph.items():
            out_msgs = superstep_fn(v, state[v], [], neighbors)
            # superstep_fn возвращает вес, который надо разделить по соседям
            if neighbors:
                share = out_msgs / len(neighbors)
                for n in neighbors:
                    inbox[n].append(share)

        # обновляем состояние на основе входящих сообщений
        new_state: Dict[VertexId, float] = {}
        for v, messages in inbox.items():
            # передаём в superstep_fn входящие сообщения (второй вызов)
            new_val = superstep_fn(v, state[v], messages, graph[v])
            new_state[v] = new_val

        state = new_state

    return state
