// place files you want to import through the `$lib` alias in this folder.

export const displayDuration = (seconds: number): string => {
	const hours = Math.floor(seconds / 3600);
	const minutes = Math.floor((seconds % 3600) / 60);
	if (hours > 0) {
		return `${hours}h${minutes.toString().padStart(2, '0')}m`;
	} else {
		return `${minutes}m`;
	}
};
