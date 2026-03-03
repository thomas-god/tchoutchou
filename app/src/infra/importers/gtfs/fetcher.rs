use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// Errors that can occur while downloading or extracting a remote GTFS archive.
#[derive(Debug)]
pub enum GTFSFetchError {
    Http(reqwest::Error),
    Zip(zip::result::ZipError),
    Io(std::io::Error),
}

impl std::fmt::Display for GTFSFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GTFSFetchError::Http(e) => write!(f, "HTTP error downloading GTFS archive: {e}"),
            GTFSFetchError::Zip(e) => write!(f, "Error extracting GTFS zip archive: {e}"),
            GTFSFetchError::Io(e) => write!(f, "I/O error during GTFS archive extraction: {e}"),
        }
    }
}

impl std::error::Error for GTFSFetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GTFSFetchError::Http(e) => Some(e),
            GTFSFetchError::Zip(e) => Some(e),
            GTFSFetchError::Io(e) => Some(e),
        }
    }
}

impl From<reqwest::Error> for GTFSFetchError {
    fn from(e: reqwest::Error) -> Self {
        GTFSFetchError::Http(e)
    }
}

impl From<zip::result::ZipError> for GTFSFetchError {
    fn from(e: zip::result::ZipError) -> Self {
        GTFSFetchError::Zip(e)
    }
}

impl From<std::io::Error> for GTFSFetchError {
    fn from(e: std::io::Error) -> Self {
        GTFSFetchError::Io(e)
    }
}

/// An extracted GTFS archive on disk.
///
/// Owns the underlying [`TempDir`], which is deleted automatically when this
/// value is dropped. Keep it alive for as long as you need to read the files —
/// in particular, until after [`GTFSParser::parse`] has fully consumed the
/// directory contents.
pub struct GTFSExtractedArchive {
    // Kept alive so the directory is not deleted while callers read from it.
    _dir: TempDir,
    path: PathBuf,
}

impl GTFSExtractedArchive {
    /// Path to the directory containing the extracted GTFS `.txt` files.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Downloads and extracts a remote GTFS zip archive.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use app::infra::importers::gtfs::{fetcher::GTFSFetcher, parsers::GTFSParser};
///
/// let archive = GTFSFetcher::fetch("https://example.com/gtfs.zip").await?;
/// let parser  = GTFSParser::parse(archive.path().to_str().unwrap())?;
/// # Ok(())
/// # }
/// ```
pub struct GTFSFetcher;

impl GTFSFetcher {
    /// Download the GTFS zip at `url`, extract it into a temporary directory,
    /// and return a [`GTFSExtractedArchive`] whose lifetime guards that
    /// directory.
    pub async fn fetch(url: &str) -> Result<GTFSExtractedArchive, GTFSFetchError> {
        let bytes = reqwest::get(url)
            .await
            .map_err(GTFSFetchError::Http)?
            .bytes()
            .await
            .map_err(GTFSFetchError::Http)?;

        let tmp_dir = tempfile::tempdir().map_err(GTFSFetchError::Io)?;

        let cursor = std::io::Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(cursor).map_err(GTFSFetchError::Zip)?;
        archive
            .extract(tmp_dir.path())
            .map_err(GTFSFetchError::Zip)?;

        let path = tmp_dir.path().to_path_buf();
        Ok(GTFSExtractedArchive {
            _dir: tmp_dir,
            path,
        })
    }
}
