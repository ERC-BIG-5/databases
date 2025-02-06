from typing import TYPE_CHECKING

from sqlalchemy import select

from databases.model_conversion import PostModel

if TYPE_CHECKING:
    from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost


def filter_posts_with_existing_post_ids(posts: list[DBPost | PostModel], db_mgmt: "DatabaseManager") -> list[
    DBPost | PostModel]:
    post_ids = [p.platform_id for p in posts]
    with db_mgmt.get_session() as session:
        query = select(DBPost.platform_id).where(DBPost.platform_id.in_(post_ids))
        found_post_ids = session.execute(query).scalars().all()
        db_mgmt.logger.debug(f"filter out posts with ids: {found_post_ids}")
    return list(filter(lambda p: p.platform_id not in found_post_ids, posts))
