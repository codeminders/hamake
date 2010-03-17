package com.codeminders.hamake;

import java.util.*;


class NoDepsExecutionGraph implements ExecutionGraph {		

	public class GraphNode {
		
		public GraphNode(Task t, int l) {
			this.task = t;
			this.level = l;
			this.done = false;
		}

		private Task task;
		private List<GraphNode> children = new ArrayList<GraphNode>();
		private int level;
		private boolean done;
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
		public void done(){
			this.done = true;
		}
		public boolean isDone(){
			return this.done;
		}
		
	}

    private List<GraphNode> rootNodes = new ArrayList<GraphNode>();

    public NoDepsExecutionGraph(List<Task> tasks) {
        buildGraph(tasks);
    }

    public void removeTask(String name) {
    	for(GraphNode node : rootNodes){
    		if(!node.getTask().getName().equalsIgnoreCase(name)){
    			for(GraphNode childNode : node.getChildren()){
    				removeTask(childNode.getTask().getName());
    			}
    		}
    		else{
    			node.done();
    		}
    	}
    }
    
    public List<String> getReadyForRunTasks() {
        List<String> ret = new ArrayList<String>();
        for(GraphNode node : rootNodes){
    		if(node.isDone()){
    			for(GraphNode childNode : node.getChildren()){
    				getReadyForRunTasks(childNode, ret);
    			}
    		}
    		else{
    			ret.add(node.getTask().getName());
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
			ret.add(node.getTask().getName());
		}
	}        
    
    private void buildGraph(List<Task> tasks){
    	List<Task> t = new ArrayList<Task>(tasks); 
    	Collections.copy(t, tasks);
    	rootNodes = fetchRootNodes(t);
    	if(rootNodes.size() > 0){
    		fetchChildren(rootNodes, t);
    	}
    }
    
    private List<GraphNode> fetchRootNodes(List<Task> tasks){
    	List<GraphNode> rootNodes = new ArrayList<GraphNode>();
    	List<Task> toRemove = new ArrayList<Task>();    	
    	for(Task ti : tasks){
    		boolean rootNode = true;
    		for(Task tj : tasks){
    			rootNode = ti.dependsOn(tj)? false : rootNode;    			   		
    		}
    		if(rootNode){
    			rootNodes.add(new GraphNode(ti, 0));
    			toRemove.add(ti);
    		}
    	}
    	tasks.removeAll(toRemove);
    	return rootNodes;
    }
    
    private void fetchChildren(List<GraphNode> nodes, List<Task> tasks){
    	//find all dependent nodes in this level
    	List<Task> toRemove = new ArrayList<Task>();
    	for(GraphNode node : nodes){
    		for(Task task : tasks){
    			  if(task.dependsOn(node.getTask())){
    				  node.addChild(new GraphNode(task, node.getLevel() + 1));
    				  toRemove.add(task);
    			  }
    		}
    	}    
    	if(!toRemove.isEmpty()){
	    	tasks.removeAll(toRemove);
	    	if(!tasks.isEmpty()){
	    		//fire this method for the next level
		    	List<GraphNode> nextLevelNodes = new ArrayList<GraphNode>();
		    	for(GraphNode node : nodes){
		    		nextLevelNodes.addAll(node.getChildren());
		    	}
		    	fetchChildren(nextLevelNodes, tasks);
	    	}
    	}
    }        
}