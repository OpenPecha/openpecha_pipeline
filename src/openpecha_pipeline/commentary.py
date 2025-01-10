from pathlib import Path
from typing import Dict, List, Union

from openpecha.alignment.serializers.translation import TextTranslationSerializer
from openpecha.config import JSON_OUTPUT_PATH, PECHAS_PATH
from openpecha.pecha.parsers.google_doc.commentary import GoogleDocCommentaryParser
from pecha_uploader.config import Destination_url
from pecha_uploader.pipeline import upload_root


def parse_commentary(
    docx_file: Path, metadata: Path, source_path: Union[str, None] = None
):
    """
    Parse translation from google docx and google sheet(metadata)
    There are two types
        i)  Root text
        ii) Root text's translation(need source_path arg i.e root text's layer path)
    """
    parser = GoogleDocCommentaryParser(source_path)
    pecha, layer_path = parser.parse(
        input=docx_file, metadata=metadata, output_path=PECHAS_PATH
    )
    return pecha, layer_path


def serialize_translation(
    bo_pecha_path: Path, en_pecha_path: Path, json_output_path: Path
):
    """
    Serialize the root opf and translation opf to JSON
    """
    serializer = TextTranslationSerializer()
    json_output_path = serializer.serialize(
        root_opf_path=bo_pecha_path,
        translation_opf_path=en_pecha_path,
        output_path=json_output_path,
        is_pecha_display=False,
    )
    return json_output_path


def prepare_commentary(root_links: Dict, source_path: Union[str, None] = None):
    """
    This is to parse and and publish the root text
    """
    root_docx_path, root_sheet_path = Path(root_links["docx"]), Path(
        root_links["sheet"]
    )

    root_pecha, root_layer_path = parse_commentary(
        root_docx_path, root_sheet_path, source_path
    )
    asset_path = root_docx_path.parent
    root_pecha.publish(asset_path=Path(asset_path), asset_name="google_docx")

    return root_pecha, root_layer_path


def get_root_metadata(root_opf_path):
    pass


def commentary_pipeline(
    root_opf_path: str,
    commentary_paths: Union[List[Dict], Dict, None] = None,
    output_path: Path = JSON_OUTPUT_PATH,
    destination_url: Destination_url = Destination_url.STAGING,
):
    # Root text pipeline
    root_metadata, root_layer_path = get_root_metadata(root_opf_path)

    # Translation text pipeline
    if isinstance(commentary_paths, Dict):
        commentary_pecha, _ = prepare_commentary(commentary_paths, str(root_layer_path))
        json_file = serialize_translation(
            root_metadata, commentary_pecha.pecha_path, output_path
        )
        upload_root(json_file, destination_url)

    elif isinstance(commentary_paths, List):
        for translation_path in commentary_paths:
            commentary_pecha, _ = prepare_commentary(
                translation_path, str(root_layer_path)
            )
            json_file = serialize_translation(
                root_metadata, commentary_pecha.pecha_path, output_path
            )
            upload_root(json_file, destination_url)

    else:
        # Update serialize_translation to handle this case
        pass
