from tap_ms_graph.streams.users import Users
from tap_ms_graph.streams.groups import Groups
from tap_ms_graph.streams.group_member import GroupMember
from tap_ms_graph.streams.group_owner import GroupOwner
from tap_ms_graph.streams.directory_roles import DirectoryRoles
from tap_ms_graph.streams.directory_role_member import DirectoryRoleMember
from tap_ms_graph.streams.applications import Applications
from tap_ms_graph.streams.service_principals import ServicePrincipals
from tap_ms_graph.streams.conditional_access_policies import ConditionalAccessPolicies
from tap_ms_graph.streams.audit_logs_signins import AuditLogsSignins
from tap_ms_graph.streams.audit_logs_directory import AuditLogsDirectory
from tap_ms_graph.streams.teams import Teams
from tap_ms_graph.streams.team_member import TeamMember
from tap_ms_graph.streams.channel import Channel

STREAMS = {
    "users": Users,
    "groups": Groups,
    "group_member": GroupMember,
    "group_owner": GroupOwner,
    "directory_roles": DirectoryRoles,
    "directory_role_member": DirectoryRoleMember,
    "applications": Applications,
    "service_principals": ServicePrincipals,
    "conditional_access_policies": ConditionalAccessPolicies,
    "audit_logs_signins": AuditLogsSignins,
    "audit_logs_directory": AuditLogsDirectory,
    "teams": Teams,
    "team_member": TeamMember,
    "channel": Channel,
}
