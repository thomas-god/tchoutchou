import { query } from '$app/server';
import z from 'zod';
import { fetchStopSchedules } from './api/stop_schedules';

const schema = z.object({
	stop: z.string(),
	from: z.string()
});

export const fetchStopSchedulesQuery = query(schema, fetchStopSchedules);
