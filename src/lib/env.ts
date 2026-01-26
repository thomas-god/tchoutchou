import { readFileSync } from 'fs';
import { env } from '$env/dynamic/private';

export function getEnv(key: string): string {
	// First, check if the env var exists directly
	const directValue = env[key];
	if (directValue !== undefined) {
		return directValue;
	}

	// Check if there's a file path in <KEY>_FILE
	const filePathKey = `${key}_FILE`;
	const filePath = env[filePathKey];

	if (filePath) {
		try {
			// Read and trim the file content
			return readFileSync(filePath, 'utf-8').trim();
		} catch (error) {
			throw new Error(
				`Failed to read environment variable from file: ${filePathKey}=${filePath}. ${error}`
			);
		}
	}

	throw new Error(
		`Environment variable '${key}' not found (checked both ${key} and ${filePathKey})`
	);
}
