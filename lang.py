import requests
from typing import TypedDict, Optional
from langgraph.graph import StateGraph

# ---- CONFIGURATION ----
API_BASE = "https://your-api-domain.com"  # <--- Replace with your actual API base URL

# ---- STATE DEFINITION ----
class State(TypedDict):
    session_id: str
    prompt: str
    profile: str
    spv_name: Optional[str]
    spv_metadata: Optional[str]
    instruction: Optional[str]
    sql_query: Optional[str]
    insight_xml: Optional[str]
    natural_response: Optional[str]

# ---- NODE DEFINITIONS ----
def identify_spv(state: State) -> State:
    resp = requests.post(f"{API_BASE}/Identify_SPV", json={
        "prompt": state["prompt"],
        "profile": state["profile"]
    })
    resp.raise_for_status()
    spv_name = resp.json()["spv_name"]
    return {"spv_name": spv_name}

def get_metadata(state: State) -> State:
    resp = requests.post(f"{API_BASE}/Get_metadata", json={
        "profile": state["profile"],
        "spv": state["spv_name"]
    })
    resp.raise_for_status()
    data = resp.json()
    return {
        "spv_metadata": data["spv_metadata"],
        "instruction": data["instruction"]
    }

def generate_sql(state: State) -> State:
    resp = requests.post(f"{API_BASE}/Generate_SQL", json={
        "instruction": state["instruction"],
        "profile": state["profile"],
        "prompt": state["prompt"]
    })
    resp.raise_for_status()
    return {"sql_query": resp.json()["sql_query"]}

def sanitise_sql(state: State) -> State:
    resp = requests.post(f"{API_BASE}/SANITISE_SQL", json={
        "sql_query": state["sql_query"]
    })
    resp.raise_for_status()
    return {"sql_query": resp.json().get("sanitised_sql", state["sql_query"])}

def generate_insights(state: State) -> State:
    resp = requests.post(f"{API_BASE}/GENERATE_INSIGHTS", json={
        "sql_query": state["sql_query"]
    })
    resp.raise_for_status()
    return {"insight_xml": resp.json()["insight_xml"]}

def generate_natural_response(state: State) -> State:
    resp = requests.post(f"{API_BASE}/Generate_NATURAL_RESPONSE", json={
        "prompt": state["prompt"],
        "sql_query": state["sql_query"]
    })
    resp.raise_for_status()
    return {"natural_response": resp.json()["natural_response"]}

# ---- GRAPH DEFINITION ----
def build_graph():
    graph = StateGraph(State)
    graph.add_node("Identify_SPV", identify_spv)
    graph.add_node("Get_metadata", get_metadata)
    graph.add_node("Generate_SQL", generate_sql)
    graph.add_node("SANITISE_SQL", sanitise_sql)
    graph.add_node("GENERATE_INSIGHTS", generate_insights)
    graph.add_node("GENERATE_NATURAL_RESPONSE", generate_natural_response)

    graph.add_edge("Identify_SPV", "Get_metadata")
    graph.add_edge("Get_metadata", "Generate_SQL")
    graph.add_edge("Generate_SQL", "SANITISE_SQL")
    graph.add_edge("SANITISE_SQL", "GENERATE_INSIGHTS")
    graph.add_edge("GENERATE_INSIGHTS", "Generate_NATURAL_RESPONSE")

    graph.set_entry_point("Identify_SPV")
    graph.set_finish_point("Generate_NATURAL_RESPONSE")
    return graph.compile()

# ---- MAIN EXECUTION ----
if __name__ == "__main__":
    input_state = {
        "session_id": "your-session-id",
        "prompt": "Enter your prompt here",
        "profile": "Enter profile here",
        "spv_name": None,
        "spv_metadata": None,
        "instruction": None,
        "sql_query": None,
        "insight_xml": None,
        "natural_response": None
    }

    workflow = build_graph()
    result = workflow.invoke(input_state)

    print("\n--- Output ---")
    print("SQL Query:\n", result["sql_query"])
    print("Insight XML:\n", result["insight_xml"])
    print("Natural Response:\n", result["natural_response"])
