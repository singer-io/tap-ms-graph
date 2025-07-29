from tap_ms_graph.streams.applications import Applications
from tap_ms_graph.streams.audit_logs_directory import AuditLogsDirectory
from tap_ms_graph.streams.audit_logs_signins import AuditLogsSignins
from tap_ms_graph.streams.calendar_events import CalendarEvents
from tap_ms_graph.streams.channel import Channel
from tap_ms_graph.streams.chat_messages import ChatMessages
from tap_ms_graph.streams.chats import Chats
from tap_ms_graph.streams.conditional_access_policies import ConditionalAccessPolicies
from tap_ms_graph.streams.contacts import Contacts
from tap_ms_graph.streams.directory_role_member import DirectoryRoleMember
from tap_ms_graph.streams.directory_role_templates import DirectoryRoleTemplates
from tap_ms_graph.streams.directory_roles import DirectoryRoles
from tap_ms_graph.streams.drive_items import DriveItems
from tap_ms_graph.streams.drives import Drives
from tap_ms_graph.streams.group_member import GroupMember
from tap_ms_graph.streams.group_owner import GroupOwner
from tap_ms_graph.streams.groups import Groups
from tap_ms_graph.streams.mail_messages import MailMessages
from tap_ms_graph.streams.service_principals import ServicePrincipals
from tap_ms_graph.streams.team_member import TeamMember
from tap_ms_graph.streams.teams import Teams
from tap_ms_graph.streams.users import Users

STREAMS = {
    "applications": Applications,
    "audit_logs_directory": AuditLogsDirectory,
    "audit_logs_signins": AuditLogsSignins,
    # "calendar_events": CalendarEvents,
    "channel": Channel,
    # "chat_messages": ChatMessages,
    "chats": Chats,
    "conditional_access_policies": ConditionalAccessPolicies,
    # "contacts": Contacts,
    "directory_role_member": DirectoryRoleMember,
    "directory_role_templates": DirectoryRoleTemplates,
    "directory_roles": DirectoryRoles,
    # "drive_items": DriveItems,
    "drives": Drives,
    "group_member": GroupMember,
    "group_owner": GroupOwner,
    "groups": Groups,
    # "mail_messages": MailMessages,
    "service_principals": ServicePrincipals,
    "team_member": TeamMember,
    "teams": Teams,
    # "users": Users,
}
