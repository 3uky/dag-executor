class DirectAcyclicGraph:
    def __init__(self):
        self.nodes = {}

    def __str__(self):
        graph_str = ""
        for source_node, destination_nodes in self.nodes.items():
            graph_str += f"{source_node} -> {', '.join(map(str, destination_nodes))}\n"
        return graph_str

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes[node] = set()

    def add_edge(self, u, v):
        if u not in self.nodes:
            self.add_node(u)
        if v not in self.nodes:
            self.add_node(v)

        self.nodes[u].add(v)

    def remove_edge(self, u, v):
        if u in self.nodes and v in self.nodes[u]:
            self.nodes[u].remove(v)

    def remove_node(self, node):
        # Remove a node and all edges associated with it
        if node in self.nodes:
            del self.nodes[node]

        # Also, remove the node from all other nodes' adjacency lists
        for u in list(self.nodes):
            if node in self.nodes[u]:
                self.nodes[u].remove(node)

    def get_nodes(self):
        return self.nodes.keys()

    def get_output_nodes(self, node):
        if node in self.nodes:
            return self.nodes[node]

    def get_input_nodes(self, node):
        return [u for u in self.nodes if node in self.nodes[u]]

    def get_nodes_without_input_edge(self):
        return [node for node in self.nodes if not any(node in self.nodes[u] for u in self.nodes)]

    def is_acyclic(self):
        """Detects whether the graph has a cycle. Returns True if the graph is acyclic, otherwise False."""

        # State tracking for DFS
        UNVISITED = 0
        VISITING = 1
        VISITED = 2

        state = {node: UNVISITED for node in self.nodes}

        def dfs(state):
            if state[node] == VISITING:
                # Cycle detected
                return False
            if state[node] == VISITED:
                # Already fully processed node
                return True

            # Mark the node as visiting (part of the current recursion stack)
            state[node] = VISITING

            # Recursively visit all adjacent nodes
            for neighbor in self.nodes.get(node, []):
                if not dfs(neighbor):
                    return False

            # Mark the node as visited (fully processed)
            state[node] = VISITED
            return True

        # Run DFS for every node in the graph
        for node in self.nodes:
            if state[node] == UNVISITED:
                if not dfs(node):
                    return False  # If any cycle is detected

        return True  # If no cycles are detected
