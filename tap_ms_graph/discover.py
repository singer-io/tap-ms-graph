import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_ms_graph.schema import get_schemas
from tap_ms_graph.streams import STREAMS
from tap_ms_graph.exceptions import MsGraphForbiddenError

LOGGER = singer.get_logger()


def discover(client=None) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.

    When a ``client`` is provided, a lightweight permission check is performed
    for every top-level stream before it is added to the catalog:

    * If the check returns 403 Forbidden the stream (and any of its children)
      is silently excluded from the catalog and a WARNING is logged.
    * If **no** top-level stream is accessible at all, a ``MsGraphForbiddenError``
      is raised so the connection attempt fails with a clear message.

    Child streams (those whose ``parent`` attribute is non-empty) inherit their
    parent's accessibility: they are included only when the parent stream passed
    its own permission check.
    """
    schemas, field_metadata = get_schemas()

    # ------------------------------------------------------------------
    # First pass: check access for every top-level stream (no parent).
    # Build the set of accessible top-level stream names so that child
    # streams can be filtered in the second pass.
    # ------------------------------------------------------------------
    accessible_top_level = set()   # top-level streams that passed the check
    inaccessible_top_level = []    # top-level streams that returned 403

    if client:
        top_level_stream_names = {
            name for name, cls in STREAMS.items() if not cls.parent
        }
        for stream_name in top_level_stream_names:
            stream_cls = STREAMS[stream_name]
            try:
                stream_cls.check_access(client)
                accessible_top_level.add(stream_name)
            except MsGraphForbiddenError:
                LOGGER.warning(
                    "Stream '%s' is not accessible (HTTP 403 Forbidden). "
                    "The account credentials do not have read permission for this endpoint. "
                    "It will be excluded from the catalog.",
                    stream_name,
                )
                inaccessible_top_level.append(stream_name)

        # If every top-level stream is inaccessible, fail early.
        if inaccessible_top_level and not accessible_top_level:
            raise MsGraphForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have "
                "'read' access to any of the streams supported by the tap. "
                "Data collection cannot be initiated due to lack of permissions."
            )

        if inaccessible_top_level:
            LOGGER.warning(
                "The account credentials supplied do not have 'read' access to the "
                "following stream(s): %s. The data for these streams would not be "
                "collected due to lack of required permission.",
                ", ".join(sorted(inaccessible_top_level)),
            )

    # ------------------------------------------------------------------
    # Second pass: build the catalog, skipping inaccessible streams.
    # ------------------------------------------------------------------
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if client:
            stream_cls = STREAMS.get(stream_name)
            if stream_cls:
                if stream_cls.parent:
                    # Child stream: include only when the parent is accessible.
                    if stream_cls.parent not in accessible_top_level:
                        LOGGER.debug(
                            "Excluding child stream '%s' because its parent stream '%s' "
                            "is not accessible.",
                            stream_name,
                            stream_cls.parent,
                        )
                        continue
                else:
                    # Top-level stream: include only when it passed the check.
                    if stream_name not in accessible_top_level:
                        continue

        try:
            schema = Schema.from_dict(schema_dict)
            stream_metadata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(f"Error processing stream '{stream_name}'")
            LOGGER.error(f"type schema_dict: {type(schema_dict)}")
            raise err

        key_properties = metadata.to_map(stream_metadata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=stream_metadata,
            )
        )

    return catalog
