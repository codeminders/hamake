package com.codeminders.hamake;

import java.util.*;

class NoDepsExecutionGraph implements ExecutionGraph {

	public class GraphNode {

		private Task task;
		private List<GraphNode> children = new ArrayList<GraphNode>();
		private List<GraphNode> parents = new ArrayList<GraphNode>();
		private boolean done;

		public GraphNode(Task t) {
			this.task = t;
			this.done = false;
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
		
		public GraphNode getRootParent(){
			for(GraphNode node : parents){
				return node.getRootParent(); 
			}			
			return this;
		}
		
	}

	private List<GraphNode> rootNodes = new ArrayList<GraphNode>();
	private Map<String, GraphNode> hash = new HashMap<String, GraphNode>();

	/**
	 * Constructor
	 * @param tasks list of tasks
	 */
	public NoDepsExecutionGraph(List<Task> tasks) {
		buildGraph(tasks);
	}

	/**
	 * Mark task as done
	 * @param name name of the task
	 */
	public void removeTask(String name) {
		if (hash.containsKey(name)) {
			hash.get(name).done();
		}
	}

	/**
	 * Get all tasks that are ready and have targets
	 * @param targets array of targets 
	 */
	public List<String> getReadyForRunTasks(String[] targets) {
		//retrieve all root nodes
		Map<String, GraphNode> nodes = new HashMap<String, GraphNode>();
		for(String target : targets){
			if(hash.containsKey(target)){
				GraphNode node = hash.get(target).getRootParent();
				nodes.put(node.getTask().getName(), node);
			}
		}
		if(nodes.size() > 0){
			return getReadyForRunTasks(new ArrayList<GraphNode>(nodes.values()));
		}
		else{
			return getReadyForRunTasks();
		}
	}
	
	/**
	 * Get all tasks that are ready 
	 */
	public List<String> getReadyForRunTasks() {
		return getReadyForRunTasks(rootNodes);
	}
	
	private List<String> getReadyForRunTasks(List<GraphNode> nodes) {
		List<String> ret = new ArrayList<String>();		
		for (GraphNode node : nodes) {
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
					GraphNode newNode = new GraphNode(ti);
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
						GraphNode newNode = new GraphNode(task);
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