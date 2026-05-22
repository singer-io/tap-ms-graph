import requests
import singer
from typing import Set
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_ms_graph.schema import get_schemas
from tap_ms_graph.streams import STREAMS
from tap_ms_graph.exceptions import MsGraphError, MsGraphForbiddenError, MsGraphUnauthorizedError, MsGraphBadRequestError

LOGGER = singer.get_logger()


def _probe_child_access(client, stream_cls, stream_name) -> Set[str]:
    """Fetches one sample record from a parent stream and checks whether each
    of its child stream endpoints is accessible.  Returns the set of child
    stream names that are NOT accessible."""
    inaccessible = set()
    if not stream_cls.children:
        return inaccessible

    parent_endpoint = f"{client.base_url}/{stream_cls.path}"
    try:
        params = {"$top": "1"} if stream_cls.supports_top else {}
        response = client.get(parent_endpoint, params, stream_cls.headers)
        parent_records = response.get(stream_cls.data_key, [])
        if not parent_records:
            return inaccessible
    except (MsGraphError, MsGraphUnauthorizedError, requests.exceptions.RequestException) as e:
        LOGGER.warning(
            "Could not fetch a sample record for '%s'; skipping child stream access checks: %s",
            stream_name,
            e,
        )
        return inaccessible

    sample_parent = parent_records[0]
    for child_stream_name in stream_cls.children:
        child_cls = STREAMS.get(child_stream_name)
        if not child_cls:
            continue
        try:
            child_cls.check_access(client, parent_record=sample_parent)
        except (MsGraphForbiddenError, MsGraphUnauthorizedError):
            LOGGER.warning(
                "Child stream '%s' is not accessible and will be excluded from the catalog.",
                child_stream_name,
            )
            inaccessible.add(child_stream_name)
        except MsGraphBadRequestError as e:
            error_msg = str(e).lower()
            if any(phrase in error_msg for phrase in (
                "license",
                "does not have",
                "not supported",
                "application-only",
            )):
                LOGGER.warning(
                    "Child stream '%s' is not accessible and will be excluded from the catalog.",
                    child_stream_name,
                )
                inaccessible.add(child_stream_name)

    return inaccessible



def _check_top_level_access(client, stream_name, stream_cls, accessible_top_level, inaccessible_child_streams) -> bool:
    """Checks access for a top-level stream and probes its children.
    Returns True if the stream is accessible and should be added to the catalog."""
    try:
        stream_cls.check_access(client)
        accessible_top_level.add(stream_name)
    except (MsGraphForbiddenError, MsGraphUnauthorizedError):
        LOGGER.warning(
            "Stream '%s' is not accessible and will be excluded from the catalog.",
            stream_name,
        )
        return False
    except MsGraphBadRequestError as e:
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in (
            "license",
            "does not have",
            "not supported",
            "application-only",
        )):
            LOGGER.warning(
                "Stream '%s' is not accessible and will be excluded from the catalog.",
                stream_name,
            )
            return False
        raise

    inaccessible_child_streams |= _probe_child_access(client, stream_cls, stream_name)
    return True


def _is_child_stream_accessible(stream_name, stream_cls, accessible_top_level, inaccessible_child_streams) -> bool:
    """Returns True if the child stream should be included in the catalog."""
    if stream_cls.parent not in accessible_top_level:
        LOGGER.debug(
            "Excluding child stream '%s' because its parent stream '%s' is not accessible.",
            stream_name, stream_cls.parent,
        )
        return False
    if stream_name in inaccessible_child_streams:
        LOGGER.debug(
            "Excluding child stream '%s' because it failed its access check.",
            stream_name,
        )
        return False
    return True


def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    """
    schemas, field_metadata = get_schemas()

    accessible_top_level: Set[str] = set()
    inaccessible_child_streams: Set[str] = set()
    catalog = Catalog([])

    # Sort so that top-level streams are always processed before their children,
    # ensuring parent access results are available when child streams are reached.
    sorted_streams = sorted(
        schemas.items(),
        key=lambda item: bool(STREAMS.get(item[0]) and STREAMS[item[0]].parent),
    )

    for stream_name, schema_dict in sorted_streams:
        stream_cls = STREAMS.get(stream_name)

        if stream_cls is None:
            # Stream exists in schemas but has no registered class; skip since
            # we cannot verify access.
            continue
        if not stream_cls.parent:
            # ---- Top-level stream: check access, then probe children ----
            if not _check_top_level_access(client, stream_name, stream_cls, accessible_top_level, inaccessible_child_streams):
                continue
        else:
            # ---- Child stream: skip if parent or self is inaccessible ----
            if not _is_child_stream_accessible(stream_name, stream_cls, accessible_top_level, inaccessible_child_streams):
                continue

        try:
            schema = Schema.from_dict(schema_dict)
            stream_metadata = field_metadata[stream_name]
            mdata_map = metadata.to_map(stream_metadata)
            key_properties = mdata_map.get((), {}).get("table-key-properties")
            stream_metadata = metadata.to_list(mdata_map)
            catalog.streams.append(
                CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=stream_metadata,
            ))
        except Exception as err:
            LOGGER.error(f"Error processing stream '{stream_name}'")
            raise err

    if not catalog.streams:
        LOGGER.error(
            "Discovery returned an empty catalog: no streams are accessible with the "
            "provided credentials. Verify that the service principal / user has the "
            "required Microsoft Graph API permissions."
        )
        raise MsGraphForbiddenError(
            "No streams are accessible. Check that the configured credentials have "
            "the required Microsoft Graph API permissions."
        )

    return catalog
