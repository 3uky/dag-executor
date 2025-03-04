import pytest

from graph import DirectAcyclicGraph

class TestDAG:
    def test_add_node(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')

        assert 'A' in dag.get_nodes()

    def test_add_edge(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')
        dag.add_node('B')
        dag.add_edge('A', 'B')

        assert 'B' in dag.get_output_nodes('A')
        assert 'A' in dag.get_input_nodes('B')

    def test_remove_edge(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')
        dag.add_node('B')
        dag.add_edge('A', 'B')
        dag.remove_edge('A', 'B')

        assert 'B' not in dag.get_output_nodes('A')
        assert 'A' not in dag.get_input_nodes('B')

    def test_remove_node(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')
        dag.add_node('B')
        dag.add_edge('A', 'B')
        dag.remove_node('A')

        assert 'A' not in dag.get_nodes()
        assert 'B' not in dag.get_input_nodes('A')  # A should not be in B's input nodes anymore

    def test_get_nodes_without_input_edge(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')
        dag.add_node('B')
        dag.add_edge('A', 'B')

        nodes_without_input = dag.get_nodes_without_input_edge()
        assert 'A' in nodes_without_input
        assert 'B' not in nodes_without_input  # B has an incoming edge from A

    def test_edge_case_node_not_exist(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')

        assert dag.get_output_nodes('A') == set()  # No outgoing edges
        assert dag.get_input_nodes('A') == []  # No incoming edges
        assert dag.get_nodes_without_input_edge() == ['A']  # A has no incoming edges

    def test_add_edge_for_non_existent_nodes(self):
        dag = DirectAcyclicGraph()
        dag.add_edge('A', 'B')  # Automatically adds A and B nodes

        assert 'A' in dag.get_nodes()
        assert 'B' in dag.get_nodes()
        assert 'B' in dag.get_output_nodes('A')
        assert 'A' in dag.get_input_nodes('B')

    def test_multiple_edges(self):
        dag = DirectAcyclicGraph()
        dag.add_node('A')
        dag.add_node('B')
        dag.add_node('C')
        dag.add_edge('A', 'B')
        dag.add_edge('A', 'C')

        assert 'B' in dag.get_output_nodes('A')
        assert 'C' in dag.get_output_nodes('A')
        assert 'A' in dag.get_input_nodes('B')
        assert 'A' in dag.get_input_nodes('C')

    def test_graph_has_cycle(self):
        dag = DirectAcyclicGraph()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_node("C")
        dag.add_edge("A", "B")
        dag.add_edge("B", "C")
        dag.add_edge("C", "A")  # This creates a cycle

        assert dag.is_acyclic() is False  # cycle detected

    def test_graph_is_acyclic(self):
        dag = DirectAcyclicGraph()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_edge("A", "B")

        assert dag.is_acyclic() is True  # no cycle