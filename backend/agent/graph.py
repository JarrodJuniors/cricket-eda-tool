"""LangGraph agent — orchestrates NL → SQL → DuckDB → Answer pipeline."""

from typing import Any, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from backend.agent.tools import get_schema, lookup_player, run_sql
from backend.agent.prompts import SYSTEM_PROMPT
from backend.agent.llm import get_llm


# ── State ────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    question: str
    session_id: str
    messages: list
    sql: str | None
    data: list[dict[str, Any]] | None
    answer: str | None
    sources: list[str]


# ── Nodes ────────────────────────────────────────────────────────────────────

tools = [get_schema, lookup_player, run_sql]
tool_node = ToolNode(tools)


def should_continue(state: AgentState) -> str:
    """Route: if last message has tool calls, run tools; else finish."""
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END


async def call_model(state: AgentState) -> AgentState:
    """Run the LLM with tool bindings."""
    llm = get_llm()
    llm_with_tools = llm.bind_tools(tools)

    messages = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
    response = await llm_with_tools.ainvoke(messages)

    # Extract SQL from tool calls if present
    sql = state.get("sql")
    for tc in getattr(response, "tool_calls", []):
        if tc["name"] == "run_sql":
            sql = tc["args"].get("sql", sql)

    return {**state, "messages": state["messages"] + [response], "sql": sql}


async def finalize(state: AgentState) -> AgentState:
    """Build final natural language answer from last AI message."""
    last = state["messages"][-1]
    answer = last.content if isinstance(last, AIMessage) else "I couldn't find an answer."
    return {**state, "answer": answer}


# ── Graph ────────────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    graph.add_node("model", call_model)
    graph.add_node("tools", tool_node)
    graph.add_node("finalize", finalize)

    graph.set_entry_point("model")
    graph.add_conditional_edges("model", should_continue, {"tools": "tools", END: "finalize"})
    graph.add_edge("tools", "model")
    graph.add_edge("finalize", END)
    return graph


_compiled_graph = build_graph().compile()


async def run_agent(question: str, session_id: str) -> dict[str, Any]:
    """Public entrypoint called by the API route."""
    initial_state: AgentState = {
        "question": question,
        "session_id": session_id,
        "messages": [HumanMessage(content=question)],
        "sql": None,
        "data": None,
        "answer": None,
        "sources": [],
    }
    final_state = await _compiled_graph.ainvoke(initial_state)
    return {
        "answer": final_state.get("answer", "No answer generated."),
        "sql": final_state.get("sql"),
        "data": final_state.get("data"),
        "sources": final_state.get("sources", []),
    }
