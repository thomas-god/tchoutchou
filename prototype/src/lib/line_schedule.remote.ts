import { query } from '$app/server';
import { fetchLineSchedule, schema } from './api/schedule';

export const fetchLineScheduleQuery = query(schema, fetchLineSchedule);
