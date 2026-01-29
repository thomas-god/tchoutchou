import { query } from '$app/server';
import { fetchLines } from './api/lines';

export const fetchLinesQuery = query(fetchLines);
