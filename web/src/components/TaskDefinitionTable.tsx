import type { ReactElement } from "react";
import type { TaskDefinition } from "../api/types";

export function TaskDefinitionTable({ tasks }: { tasks: TaskDefinition[] }): ReactElement {
  return <div className="table-wrap"><table><thead><tr><th>任务</th><th>执行器</th><th>Action</th><th>依赖</th><th>超时</th></tr></thead><tbody>{tasks.map((task) => <tr key={task.key}><td><code>{task.key}</code></td><td>{task.executor ?? "mock"}</td><td>{task.action}</td><td>{task.depends_on?.join(", ") || "无"}</td><td>{task.timeout_ms} ms</td></tr>)}</tbody></table></div>;
}
