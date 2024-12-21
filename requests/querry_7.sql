CREATE OR REPLACE FUNCTION validate_souvenircategories()
RETURNS TRIGGER AS $$
BEGIN
    -- Проверка, что idparent ссылается на существующую запись, если не NULL
    IF NEW.idparent IS NOT NULL THEN
        PERFORM id FROM SouvenirCategories WHERE id = NEW.idparent;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Parent ID % does not exist', NEW.idparent;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_souvenircategories
BEFORE INSERT OR UPDATE ON SouvenirCategories
FOR EACH ROW EXECUTE FUNCTION validate_souvenircategories();

INSERT INTO SouvenirCategories (idparent, name) VALUES (21412, 'Некорректная категория') RETURNING id;


DROP TRIGGER IF EXISTS trg_validate_souvenircategories ON souvenircategories;
DROP FUNCTION IF EXISTS validate_souvenircategories CASCADE;
