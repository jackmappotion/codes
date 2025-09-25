def split_path(path: str):
    """'/A/B/C' -> ['A','B','C']"""
    return [p for p in path.strip("/").split("/") if p]


def get_directory_id_by_path(gd_controller, path):
    parts = split_path(path)
    parent_id = "root"
    for name in parts:
        matches = gd_controller._find_files(name, gd_controller.MIME_TYPES['folder'], parent_id)
        if not matches:
            raise FileNotFoundError(f"폴더 '{name}' not found (path={path})")
        elif len(matches) == 1:
            parent_id = matches[0]['id']
        else:
            raise ValueError(f"Duplicate folder name '{name}' under parent {parent_id}")
    return parent_id


def get_sheet_id_by_path(gd_controller, path: str) -> str:
    parts = split_path(path)
    if not parts:
        raise ValueError("Empty path")

    # 경로가 단일 시트 이름일 경우 root 기준
    if len(parts) == 1:
        parent_id, sheet_name = "root", parts[0]
    else:
        *dirs, sheet_name = parts
        parent_id = get_directory_id_by_path(gd_controller, "/" + "/".join(dirs))

    matches = gd_controller._find_files(sheet_name, gd_controller.MIME_TYPES['sheet'], parent_id)
    if not matches:
        raise FileNotFoundError(f"시트 '{sheet_name}' not found (path={path})")
    elif len(matches) == 1:
        return matches[0]['id']
    else:
        raise ValueError(f"Duplicate sheet name '{sheet_name}' under parent {parent_id}")
