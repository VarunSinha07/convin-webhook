DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_event_id') THEN
        ALTER TABLE events ADD CONSTRAINT unique_event_id UNIQUE (event_id);
    END IF;
END $$;
