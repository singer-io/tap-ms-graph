# tap-ms-graph

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

This tap:

- Pulls raw data from the [MS_Graph API].
- Extracts the following resources:
    - [Applications](https://learn.microsoft.com/en-us/graph/api/application-list)

    - [AuditLogsDirectory](https://learn.microsoft.com/en-us/graph/api/directoryaudit-list)

    - [AuditLogsSignins](https://learn.microsoft.com/en-us/graph/api/signin-list)

    - [CalendarEvents](https://learn.microsoft.com/en-us/graph/api/user-list-events)

    - [Channel](https://learn.microsoft.com/en-us/graph/api/channel-list)

    - [ChatMessages](https://learn.microsoft.com/en-us/graph/api/chat-list-messages)

    - [Chats](https://learn.microsoft.com/en-us/graph/api/chat-list)

    - [ConditionalAccessPolicies](https://learn.microsoft.com/en-us/graph/api/conditionalaccessroot-list-policies)

    - [Contacts](https://learn.microsoft.com/en-us/graph/api/user-list-contacts?view=graph-rest-1.0&tabs=http)

    - [DirectoryRoleMember](https://learn.microsoft.com/en-us/graph/api/directoryrole-list-members)

    - [DirectoryRoleTemplates](https://learn.microsoft.com/en-us/graph/api/directoryroletemplate-list)

    - [DirectoryRoles](https://learn.microsoft.com/en-us/graph/api/directoryrole-list)

    - [DriveItems](https://learn.microsoft.com/en-us/graph/api/drive-get?view=graph-rest-1.0&tabs=http)

    - [Drives](https://learn.microsoft.com/en-us/graph/api/drive-list)

    - [GroupMember](https://learn.microsoft.com/en-us/graph/api/group-list-members)

    - [GroupOwner](https://learn.microsoft.com/en-us/graph/api/group-list-owners)

    - [Groups](https://learn.microsoft.com/en-us/graph/api/group-list)

    - [MailMessages](https://learn.microsoft.com/en-us/graph/api/user-list-messages)

    - [ServicePrincipals](https://learn.microsoft.com/en-us/graph/api/serviceprincipal-list)

    - [TeamMember](https://learn.microsoft.com/en-us/graph/api/team-list-members)

    - [Teams](https://learn.microsoft.com/en-us/graph/teams-list-all-teams)

    - [Users](https://learn.microsoft.com/en-us/graph/api/user-list)

- Outputs the schema for each resource
- FULL_TABLE replication pulls all records from the source every time


## Streams


[applications](https://learn.microsoft.com/en-us/graph/api/application-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[audit_logs_directory](https://learn.microsoft.com/en-us/graph/api/directoryaudit-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[audit_logs_signins](https://learn.microsoft.com/en-us/graph/api/signin-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[calendar_events](https://learn.microsoft.com/en-us/graph/api/user-list-events)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[channel](https://learn.microsoft.com/en-us/graph/api/channel-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[chat_messages](https://learn.microsoft.com/en-us/graph/api/chat-list-messages)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[chats](https://learn.microsoft.com/en-us/graph/api/chat-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[conditional_access_policies](https://learn.microsoft.com/en-us/graph/api/conditionalaccessroot-list-policies)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[contacts](https://learn.microsoft.com/en-us/graph/api/user-list-contacts?view=graph-rest-1.0&tabs=http)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[directory_role_member](https://learn.microsoft.com/en-us/graph/api/directoryrole-list-members)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[directory_role_templates](https://learn.microsoft.com/en-us/graph/api/directoryroletemplate-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[directory_roles](https://learn.microsoft.com/en-us/graph/api/directoryrole-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[drive_items](https://learn.microsoft.com/en-us/graph/api/drive-get?view=graph-rest-1.0&tabs=http)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[drives](https://learn.microsoft.com/en-us/graph/api/drive-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[group_member](https://learn.microsoft.com/en-us/graph/api/group-list-members)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[group_owner](https://learn.microsoft.com/en-us/graph/api/group-list-owners)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[groups](https://learn.microsoft.com/en-us/graph/api/group-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[mail_messages](https://learn.microsoft.com/en-us/graph/api/user-list-messages)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[service_principals](https://learn.microsoft.com/en-us/graph/api/serviceprincipal-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[team_member](https://learn.microsoft.com/en-us/graph/api/team-list-members?view=graph-rest-1.0&tabs=http)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[teams](https://learn.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#list-groups)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

[users](https://learn.microsoft.com/en-us/graph/api/user-list)
- Data Key = value
- Primary keys: ['id']
- Replication strategy: FULL_TABLE



## Prerequsite
    ```
    To use Microsoft Graph APIs for sync and data access, ensure your tenant has the required licenses.

        | **Graph API / Feature**                | **Required License**                       | **Notes**                                              |
        |----------------------------------------|--------------------------------------------|--------------------------------------------------------|
        | **Mail, Calendar, Contacts**           | M365 Business Basic or higher              | Access via `Mail.Read`, `Calendars.Read`, etc.         |
        | **Azure AD Directory Sync**            | Azure AD Premium P1 or P2                  | Needed for advanced directory features                 |
        | **Audit Logs (30–365 days)**           | AAD P1/P2 or M365 E5                       | For extended retention and compliance                  |
        | **Teams Messages (Chats, Channels)**   | M365 E5 + Microsoft approval               | Protected APIs; app access needs justification         |
        | **Webhooks / Change Notifications**    | Any M365 license                           | For real-time sync triggers                            |
        | **Data Sync APIs (Delta Queries)**     | Any M365 license                           | Available for users, groups, mail, etc.                |

    ```

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-ms-graph
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-ms-graph <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.
   
    ```json
    {
        "tenant_id": "AZURE_TENANT_ID",
        "client_id": "AZURE_CLIENT_ID",
        "client_secret": "AZURE_CLIENT_SECRET",
        "scope": "AZURE_SCOPE"
    }
    ```
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "users",
        "bookmarks": {
            "groups": "2019-09-27T22:34:39.000000Z",
            "teams": "2019-09-28T15:30:26.000000Z",
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-ms-graph --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md

    For Sync mode:
    ```bash
    > tap-ms-graph --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-ms-graph --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-ms-graph --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the ms_graph tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md
    ```bash
    > pylint tap_ms_graph -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools
    ```bash
    > tap_ms_graph --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2019 Stitch
