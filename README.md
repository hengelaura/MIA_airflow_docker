# Minneapolis Institute of Art
The current collection information stored in the Minneapolis Institute of Art's public github repository (artsmia/collection) is a representation of what pieces the institute has today and is updated on an almost daily basis.
When artworks are removed from the collection, the corresponding files are deleted, so there isn't an account of the file having existed. Artworks also have a status of being on view or not and there isn't currently any kind of history
table that would allow for finding how long pieces have been on display, when they were added to the collection, or when they were removed from the collection.


## Airflow Tasks

### list_commits
This task will retrieve a list of all the commits made to the main branch of the artsmia/collection repository. It will extract the commit sha and commit url into a dataframe where the duplicates are dropped and the results are written to
a Postgres instance.
