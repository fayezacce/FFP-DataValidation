"use client";

import { useState, useEffect } from "react";
import { Loader2, CheckCircle2, XCircle, ChevronUp, ChevronDown, Trash2 } from "lucide-react";

interface Task {
  id: string;
  task_name: string;
  status: "pending" | "running" | "completed" | "error";
  progress: number;
  message: string;
  result_url: string | null;
  error_details: string | null;
  created_at: string;
}

import { getToken, fetchWithAuth, downloadFileWithAuth } from "@/lib/auth";

export default function TaskTray() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!mounted) return;

    let interval: NodeJS.Timeout;

    const fetchTasks = async () => {
      const token = getToken();
      if (!token) return;

      try {
        const res = await fetchWithAuth("/api/tasks/my-tasks");
        if (res.ok) {
          const data = await res.json();
          setTasks(data);
          
          // Auto-open logic if there's a running task and tray is closed, we might open it
          // Or just update the unread dot
        }
      } catch (err) {
        console.error("Failed to fetch tasks", err);
      }
    };

    fetchTasks();
    interval = setInterval(fetchTasks, 5000);

    return () => clearInterval(interval);
  }, [mounted]);

  if (!mounted || tasks.length === 0) return null;

  const runningCount = tasks.filter(t => t.status === "pending" || t.status === "running").length;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col items-end">
      {isOpen && (
        <div className="w-80 bg-[#121214] border border-[#2d2f34] rounded-lg shadow-2xl mb-2 overflow-hidden flex flex-col max-h-[400px]">
          <div className="p-3 border-b border-[#2d2f34] flex justify-between items-center bg-[#1a1c20]">
            <h3 className="font-semibold text-sm text-[#e0e1e4]">Background Tasks</h3>
            {tasks.some(t => t.status === "completed" || t.status === "error") && (
              <button 
                onClick={async () => {
                  const token = getToken();
                  if (token) {
                    await fetchWithAuth("/api/tasks/cleanup", { method: "DELETE" });
                    setTasks(tasks.filter(t => t.status === "running" || t.status === "pending"));
                  }
                }}
                className="text-xs text-[#a0a3ab] hover:text-[#f8f9fa] flex items-center gap-1"
              >
                <Trash2 size={12} /> Clear Done
              </button>
            )}
          </div>
          <div className="overflow-y-auto p-2 space-y-2 flex-1">
            {tasks.map(task => (
              <div key={task.id} className="bg-[#1e2025] rounded p-3 border border-[#2d2f34]">
                <div className="flex justify-between items-start mb-1">
                  <span className="text-xs font-medium text-[#f8f9fa] capitalize">
                    {task.task_name.replace(/_/g, " ")}
                  </span>
                  <div className="flex items-center gap-2">
                    {(task.status === "running" || task.status === "pending") && (
                      <button 
                        onClick={() => handleCancel(task.id)}
                        className="text-[#a0a3ab] hover:text-red-400 transition-colors"
                      >
                        <X size={14} />
                      </button>
                    )}
                    {task.status === "running" || task.status === "pending" ? (
                      <Loader2 size={14} className="animate-spin text-blue-400" />
                    ) : task.status === "completed" ? (
                      <CheckCircle2 size={14} className="text-green-500" />
                    ) : (
                      <XCircle size={14} className="text-red-500" />
                    )}
                  </div>
                </div>
                
                <p className="text-[11px] text-[#a0a3ab] mb-2 line-clamp-2">
                  {task.message || (task.status === "completed" ? "Done" : task.status)}
                </p>

                {(task.status === "running" || task.status === "pending") && (
                  <div className="w-full bg-[#2d2f34] rounded-full h-1.5 mb-1">
                    <div 
                      className="bg-blue-500 h-1.5 rounded-full transition-all duration-500" 
                      style={{ width: `${task.progress}%` }}
                    />
                  </div>
                )}
                
                <div className="flex justify-between items-center mt-2 h-5">
                  <span className="text-[10px] text-[#71747d]">
                    {new Date(task.created_at + (task.created_at.endsWith('Z') ? '' : 'Z')).toLocaleTimeString()}
                  </span>
                  {task.result_url && (
                    <button 
                      onClick={() => downloadFileWithAuth(task.result_url!)}
                      className="text-[10px] bg-[#2d2f34] hover:bg-[#3d4047] text-white px-2 py-1 rounded transition-colors"
                    >
                      Download
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
      
      <button 
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 bg-[#1a1c20] hover:bg-[#2d2f34] border border-[#2d2f34] text-sm text-[#f8f9fa] px-4 py-2 rounded-full shadow-lg transition-colors"
      >
        {runningCount > 0 && <Loader2 size={16} className="animate-spin text-blue-400" />}
        <span>Tasks {runningCount > 0 ? `(${runningCount})` : ""}</span>
        {isOpen ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
      </button>
    </div>
  );
}
