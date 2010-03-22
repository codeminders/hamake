package com.codeminders.hamake;

import java.util.*;

class NoDepsExecutionGraph implements ExecutionGraph {

	public class GraphNode {

		private Task task;
		private List<GraphNode> children = new ArrayList<GraphNode>();
		private List<GraphNode> parents = new ArrayList<GraphNode>();
		private int level;
		private boolean done;

		public GraphNode(Task t, int l) {
			this.task = t;
			this.level = l;
			this.done = false;
		}

		public int getLevel() {
			return level;
		}

		public Task getTask() {
			return this.task;
		}

		public List<GraphNode> getChildren() {
			return children;
		}

		public void addChild(GraphNode child) {
			this.children.add(child);
		}
		public void addParent(GraphNode child) {
			this.parents.add(child);
		}

		public void done() {
			this.done = true;
		}

		public boolean isDone() {
			return this.done;
		}

		public boolean isReady() {
			for (GraphNode parent : parents) {
				if (!parent.isDone())
					return false;
			}
			return true;
		}
		
		public void makeRoot(){
			for(GraphNode node : parents){
				node.done();
			}
		}

	}

	private List<GraphNode> rootNodes = new ArrayList<GraphNode>();
	private Map<String, GraphNode> hash = new HashMap<String, GraphNode>();

	public NoDepsExecutionGraph(List<Task> tasks) {
		buildGraph(tasks);
	}

	public void removeTask(String name) {
		if (hash.containsKey(name)) {
			hash.get(name).done();
		}
	}

	public List<String> getReadyForRunTasks(String rootTask) {		
		if(hash.containsKey(rootTask)){
			List<String> ret = new ArrayList<String>();
			GraphNode task = hash.get(rootTask);
			task.makeRoot();
			getReadyForRunTasks(task, ret);
			return ret;
		}
		else{
			return getReadyForRunTasks();
		}
	}
	
	public List<String> getReadyForRunTasks() {
		List<String> ret = new ArrayList<String>();		
		for (GraphNode node : rootNodes) {
			if (node.isDone()) {
				for (GraphNode childNode : node.getChildren()) {
					getReadyForRunTasks(childNode, ret);
				}
			} else {
				if (node.isReady()) {
					if(Collections.binarySearch(ret, node.getTask().getName()) < 0){
						ret.add(node.getTask().getName());
					}
				}
			}
		}
		return ret;
	}

	private void getReadyForRunTasks(GraphNode node, List<String> ret) {
		if (node.isDone()) {
			for (GraphNode childNode : node.getChildren()) {
				getReadyForRunTasks(childNode, ret);
			}
		} else {
			if (node.isReady()) {
				if(Collections.binarySearch(ret, node.getTask().getName()) < 0){
					ret.add(node.getTask().getName());
				}
			}
		}
	}

	private void buildGraph(List<Task> tasks) {
		List<Task> t = new ArrayList<Task>(tasks);
		Collections.copy(t, tasks);
		rootNodes = fetchRootNodes(t);
		if (rootNodes.size() > 0) {
			fetchChildren(rootNodes, t);
		}
	}

	private List<GraphNode> fetchRootNodes(List<Task> tasks) {
		List<GraphNode> rootNodes = new ArrayList<GraphNode>();
		for (Task ti : tasks) {
			boolean rootNode = true;
			for (Task tj : tasks) {
				rootNode = dependsOn(ti,tj) ? false : rootNode;
			}
			if (rootNode) {
				if (!hash.containsKey(ti.getName())) {
					// add new node
					GraphNode newNode = new GraphNode(ti, 0);
					rootNodes.add(newNode);
					hash.put(ti.getName(), newNode);
				}
			}
		}
		return rootNodes;
	}

	private void fetchChildren(List<GraphNode> nodes, List<Task> tasks) {
		// find all dependent nodes in this level
		List<GraphNode> nextLevelNodes = new ArrayList<GraphNode>();
		for (GraphNode node : nodes) {
			for (Task task : tasks) {
				if (dependsOn(task,node.getTask())) {
					if (!hash.containsKey(task.getName())) {
						// add new node
						GraphNode newNode = new GraphNode(task,
								node.getLevel() + 1);
						newNode.addParent(node);
						node.addChild(newNode);
						hash.put(task.getName(), newNode);
						nextLevelNodes.add(newNode);
					}
					else{
						node.addChild(hash.get(task.getName()));
						hash.get(task.getName()).addParent(node);
					}
				}
			}
		}
		if (nextLevelNodes.size() > 0) {
			// fire this method for the next level
			fetchChildren(nextLevelNodes, tasks);
		}
	}
	
	protected boolean dependsOn(Task a, Task b){
		return a.dependsOn(b);
	}
}