function help() {

	return "Submit a task to a compute group\n" +
	        "Parameters (1): taskTypeId taskSubTypeId cgid wait ..."
	        "Parameters (2): task cgid wait\n" +
	        "Parameters (3): pathTaskFile cgid wait\n" +
	        "  taskTypeId: Type id of the task to submit\n" +
	        "  taskSubTypeId: Subtype id of the task to submit\n" +
            "  task: Task instance to submit\n" +
            "  pathTaskFile: Path to a task file\n" +
            "  cgid: Id of the compute group to submit the task to\n" +
            "  wait: Wait/block until the task is completed\n" +
            "  ...: Task arguments as further parameters depending on the task";
}

function exec(arg1) {

    // TODO create dispatching
}

function exec_taskId(taskTypeId, taskSubTypeId, cgid, wait) {

}

function exec_task(task, cgid, wait) {

}

function exec_taskFile(pathTaskFile, cgid, wait) {

}
