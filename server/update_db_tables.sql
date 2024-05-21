-- Execute these scripts to update the old schema and make it compatible with the dataloader

-- Rename tables
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'medias') THEN  
        ALTER TABLE public.cubeobjects RENAME TO medias;
    END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'taggings') THEN  
        ALTER TABLE public.objecttagrelations RENAME TO taggings;
    END IF;	
END$$;

-- Add the tag types to the tagsets table
ALTER TABLE public.tagsets ADD COLUMN IF NOT EXISTS tagtype_id integer;

DO $$
begin
IF NOT EXISTS (select 1 
				   from information_schema.constraint_column_usage 
				   where table_name = 'tag_types'  and constraint_name = 'FK_tagsets_tag_types_tagtype_id') THEN
        ALTER TABLE public.tagsets ADD CONSTRAINT "FK_tagsets_tag_types_tagtype_id" FOREIGN KEY (tagtype_id) REFERENCES public.tag_types(id) ON DELETE RESTRICT;
	END IF;
END$$;


UPDATE public.tagsets AS ts
SET tagtype_id = (
	SELECT DISTINCT t.tagtype_id
    FROM public.tags t
    WHERE t.tagset_id = ts.id
    LIMIT 1
);


-- Trigger to check that the type of tags correspond to the type of the tagset
CREATE OR REPLACE FUNCTION public.check_matching_tagtype()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.tagtype_id IS DISTINCT FROM (SELECT tagtype_id FROM public.tagsets WHERE id = NEW.tagset_id)) THEN
        RAISE EXCEPTION 'Tagtype_id does not match the corresponding tagset''s tagtype_id';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_tagtype_matching
BEFORE INSERT OR UPDATE ON public.tags
FOR EACH ROW
EXECUTE FUNCTION public.check_matching_tagtype();

-- Trigger to check, when creating a node, that the associated tag belongs to the tagset of the hierachy
CREATE OR REPLACE FUNCTION public.check_nodes_tagset()
RETURNS TRIGGER AS
$$
DECLARE
  tagset_id_for_hierarchy INTEGER;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM public.tags t 
    JOIN (SELECT tagset_id FROM public.hierarchies WHERE id = NEW.hierarchy_id) h
    ON t.tagset_id = h.tagset_id
    WHERE t.id = NEW.tag_id
  ) THEN
    RAISE EXCEPTION 'The tag_id does not belong to the correct tagset_id for this hierarchy.';
  END IF;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_check_nodes_tagset
BEFORE INSERT OR UPDATE ON public.nodes
FOR EACH ROW
EXECUTE FUNCTION public.check_nodes_tagset();