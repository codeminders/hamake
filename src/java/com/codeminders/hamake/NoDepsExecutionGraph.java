package com.codeminders.hamake;

import java.io.IOException;
import java.util.*;

import com.codeminders.hamake.dtr.DataTransformationRule;

class NoDepsExecutionGraph implements ExecutionGraph {

	public class GraphNode {

		private DataTransformationRule task;
		private List<GraphNode> children = new ArrayList<GraphNode>();
		private List<GraphNode> parents = new ArrayList<GraphNode>();
		private boolean done;

		public GraphNode(DataTransformationRule t) {
			this.task = t;
			this.done = false;
		}

		public DataTransformationRule getTask() {
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
	 * @throws IOException 
	 */
	public NoDepsExecutionGraph(List<DataTransformationRule> tasks) throws IOException {
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
	public Set<String> getReadyForRunTasks(String[] targets) {
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
	
	public List<String> getDependentTasks(String task){
		List<String> dependentTasks = new ArrayList<String>();
		if(hash.containsKey(task)){
			for(GraphNode childTask : hash.get(task).getChildren()){
				dependentTasks.add(childTask.getTask().getName());
			}
		}
		return dependentTasks;
	}
	
	/**
	 * Get all tasks that are ready 
	 */
	public Set<String> getReadyForRunTasks() {
		return getReadyForRunTasks(rootNodes);
	}
	
	private Set<String> getReadyForRunTasks(List<GraphNode> nodes) {
		Set<String> ret = new HashSet<String>();
		for (GraphNode node : nodes) {
			if (node.isDone()) {
				for (GraphNode childNode : node.getChildren()) {
					getReadyForRunTasks(childNode, ret);
				}
			} else {
				if (node.isReady()) {
                    ret.add(node.getTask().getName());
				}
			}
		}
		return ret;
	}

	private void getReadyForRunTasks(GraphNode node, Set<String> ret) {
		if (node.isDone()) {
			for (GraphNode childNode : node.getChildren()) {
				getReadyForRunTasks(childNode, ret);
			}
		} else {
			if (node.isReady()) {
                ret.add(node.getTask().getName());
			}
		}
	}

	private void buildGraph(List<DataTransformationRule> tasks) throws IOException {
		List<DataTransformationRule> t = new ArrayList<DataTransformationRule>(tasks);
		Collections.copy(t, tasks);
		rootNodes = fetchRootNodes(t);
		if (rootNodes.size() > 0) {
			fetchChildren(rootNodes, t);
		}
	}

	private List<GraphNode> fetchRootNodes(List<DataTransformationRule> tasks) throws IOException {
		List<GraphNode> rootNodes = new ArrayList<GraphNode>();
		for (DataTransformationRule ti : tasks) {
			boolean rootNode = true;
			for (DataTransformationRule tj : tasks) {
				rootNode = ti.dependsOn(tj) ? false : rootNode;
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

	private void fetchChildren(List<GraphNode> nodes, List<DataTransformationRule> tasks) throws IOException {
		// find all dependent nodes in this level
		List<GraphNode> nextLevelNodes = new ArrayList<GraphNode>();
		for (GraphNode node : nodes) {
			for (DataTransformationRule task : tasks) {
				if (!task.getName().equals(node.getTask().getName()) && task.dependsOn(node.getTask())) {
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
	
}