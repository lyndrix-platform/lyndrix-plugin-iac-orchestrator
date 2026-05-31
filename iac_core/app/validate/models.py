from pydantic import BaseModel, Field, IPvAnyAddress, ValidationError
from typing import List, Dict, Optional, Union, Any

# --- Common Sub-Models ---

class ServiceDefinition(BaseModel):
    name: str
    state: str = "present"
    deploy_type: str = "docker_compose"
    git_repo: Optional[str] = None
    git_version: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class TerraformConfig(BaseModel):
    is_managed: Optional[bool] = False
    is_used: Optional[bool] = False
    provider: Optional[str] = None
    username: Optional[str] = None
    auth_type: Optional[str] = None
    realm: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    ssh_agent: Optional[bool] = None
    ssh_key: Optional[str] = None
    node_name: Optional[str] = None
    vm_id: Optional[int] = None
    ip: Optional[str] = None
    gateway: Optional[str] = None
    nameserver: Optional[str] = None

    # Allow all extra LXC container fields (cores, ram/memory, disk/disk_size,
    # bridge, vlan, arch, swap, unprivileged, nesting, etc.) to pass through
    # without being silently dropped.
    model_config = {"extra": "allow"}

class NetworkDefinition(BaseModel):
    name: str
    ip: Union[IPvAnyAddress, str] # Accepting str to allow CIDR notations if needed

# --- Host Models ---

class StandardHost(BaseModel):
    hostname: str
    ansible_host: str
    terraform: Optional[TerraformConfig] = None
    profiles: Optional[List[str]] = Field(default_factory=list)
    ansible_groups: Optional[List[str]] = Field(default_factory=list)
    baseline_roles: Optional[List[str]] = Field(default_factory=list)
    services: Optional[List[ServiceDefinition]] = Field(default_factory=list)
    
    model_config = {"extra": "allow"}

class HardwareNode(BaseModel):
    type: str
    profiles: Optional[List[str]] = Field(default_factory=list)
    ip: Optional[Union[IPvAnyAddress, str]] = None
    ansible_host: Optional[str] = None
    roles: Optional[List[str]] = Field(default_factory=list)
    networks: Optional[List[NetworkDefinition]] = Field(default_factory=list)
    terraform: Optional[TerraformConfig] = None
    
    model_config = {"extra": "allow"}

class HardwareCluster(BaseModel):
    is_cluster: bool
    type: str
    cluster_network: Optional[str] = None
    nodes: Dict[str, HardwareNode]
    
    model_config = {"extra": "allow"}

# Hardware Host can be either a single node, or a cluster grouping nodes
HardwareHostType = Union[HardwareCluster, HardwareNode]

# --- The Root Configuration Model ---

class IaCConfiguration(BaseModel):
    """The root model that validates the deeply merged configuration dictionary."""
    global_vars: Optional[Dict[str, Any]] = None
    site_vars: Optional[Dict[str, Any]] = None
    stage_vars: Optional[Dict[str, Any]] = None
    
    hardware_hosts: Optional[Dict[str, HardwareHostType]] = Field(default_factory=dict)
    hosts: Optional[Dict[str, StandardHost]] = Field(default_factory=dict)
    
    model_config = {"extra": "allow"} # Allow other top-level keys like 'service_catalog'

def validate_configuration(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates the raw dictionary against Pydantic schemas.
    Returns the validated and properly typed dictionary, or raises an exception.
    """
    try:
        # Load the dict into the Pydantic model. This will trigger all validation rules.
        validated_model = IaCConfiguration(**config_dict)
        
        # FIX: mode='json' wandelt komplexe Python-Objekte (wie IPs) zurück in saubere Strings!
        return validated_model.model_dump(exclude_none=True, mode='json')
    
    except ValidationError as e:
        # Format the error nicely for the CLI output
        error_msgs = []
        for error in e.errors():
            loc = " -> ".join([str(x) for x in error['loc']])
            msg = error['msg']
            error_msgs.append(f"[{loc}]: {msg}")
        
        formatted_errors = "\n".join(error_msgs)
        raise ValueError(f"Data Validation Failed:\n{formatted_errors}")